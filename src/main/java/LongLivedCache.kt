package kz.technodom.inbody.util

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.timer
import kotlin.concurrent.timerTask

// share "constants"
private var INVALIDATOR_DELAY = 1000L
private var MAX_THREADS = 3

/**
 * The long lived cache for a some heavily loaded but few updatable services
 *
 * Cached entities can be connected by relation child->parents.
 * This connection guarantees that parent always be updated if child is wanted to be updated.
 *
 * All configuration should be made via [register]
 *
 * @param timeoutInSeconds in seconds, after that [invalidateAll] will be fired automatically
 *
 * @author alex@justprodev.com
 */
open class LongLivedCache(
    timeoutInSeconds: Int,
    maxThreads: Int = 3,
    invalidatorDelay: Long = 1000L
) {
    private val agents = HashMap<String, CacheAgent<*>>()
    private val log = LoggerFactory.getLogger(this::class.java)

    init {
        // "constants"
        INVALIDATOR_DELAY = invalidatorDelay
        MAX_THREADS = maxThreads

        val ttl = TimeUnit.SECONDS.toMillis(timeoutInSeconds.toLong())
        timer("${this.javaClass.name}_invalidate", initialDelay = ttl, period = ttl) {
            log.debug("invalidate ${agents.size} agents by timeout ($timeoutInSeconds seconds)")
            invalidateAll()
        }
        log.debug("init: threads = $MAX_THREADS, timeout = $ttl ms, invalidator_delay = $INVALIDATOR_DELAY ms")
    }

    /**
     * @param name should be registered with [register] before
     * @param forceUpdate update cached value and return
     */
    @Suppress("UNCHECKED_CAST")
    @Throws(MethodNotRegisteredException::class)
    fun <R> get(name: String, forceUpdate: Boolean = false): R? {
        val agent = agents[name] ?: throw MethodNotRegisteredException("Method $name not registered")
        return agent.get(forceUpdate) as R?
    }

    /**
     * @param name should be registered with [register] before
     */
    @Throws(MethodNotRegisteredException::class)
    fun invalidate(name: String) {
        val agent = agents[name] ?: throw MethodNotRegisteredException("Method $name not registered")
        agent.invalidate()
    }

    fun invalidateAll() {
        agents.keys.forEach { invalidate(it) }
    }

    /**
     *
     */
    @Throws(WrongOrderException::class)
    fun <R> register(
        name: String,
        method: ()->R,
        roots: List<String>? = null
    ) {
        // check roots
        val rootAgents = roots?.map {
            val agent = agents[it] ?: throw WrongOrderException("'$it' should be registered before '$name'")
            agent
        }
        _register(name, method, rootAgents)
    }

    /**
     * Create and register new [CacheAgent] with [name]
     * @param name for the new agent
     * @param roots agents to upgrade before upgrading agent
     * @return create agent
     */
    private fun <R> _register(
        name: String,
        method: Method<R>,
        roots: List<CacheAgent<*>>? = null
    ): CacheAgent<R> {
        log.info("register agent '$name'")
        return CacheAgent(name, method, ::log, roots).also {
            agents[name] = it
        }
    }

    private fun log(name: String, e: Throwable) {
        log.error("$name: ${e.javaClass} ${e.message}")
    }
}

/**
 * An agent that retrieves result from [updater].
 * Result will be stored until [invalidate] isn't called
 * @param R result of the [updater]
 * @param updater function that produces some result that will be cached
 * @param onException delegate the handling on an exceptions, which can be thrown by [updater]
 * @param roots related [CacheAgent]'s that will be updated by this agent after updating himself - it's guaranteed
 */
private class CacheAgent<R>(
    private val name: String,
    private val updater: Method<R>,
    private val onException: (name: String, e: Throwable)->Unit,
    private val roots: List<CacheAgent<*>>? = null
) {
    private val mutex = Mutex()
    private var job: Job? = null
    private var cached: R? = null
    private val log = LoggerFactory.getLogger(CacheAgent::class.java)
    private val weight by lazy { // roots level, 0 - no roots, 1 - roots without other roots, ...
    fun calculateWeight(prevWeight: Int, agent: CacheAgent<*>): Int {
        return agent.roots?.let { roots->
            roots.maxOf { root -> calculateWeight(prevWeight+1, root) }
        } ?: prevWeight
    }
        calculateWeight(0, this)
    }

    /**
     * Get the cached value.
     * @return may be null if some errors during the call [updater]
     */
    fun get(forceUpdate: Boolean = false): R? = runBlocking {
        val result = if(forceUpdate) null else cached

        if(result != null) return@runBlocking result

        // wait the update() any case
        mutex.withLock {
            // We want fresh update()
            if(forceUpdate) {
                job?.cancel()
                cached = null
            } else {
                // let's wait a job if it is active
                if(job?.isActive == true) try { job?.join() } catch(_: Exception) {}
            }

            fun updateWithRoots(): R? {
                update()
                roots?.forEach { it.invalidate() }
                return cached
            }

            cached ?: updateWithRoots() // update any case if we have cached==null
        }
    }

    /**
     * The shadow updating
     */
    fun invalidate() {
        log.debug("invalidate '$name'")
        Invalidator.schedule(this@CacheAgent)
    }

    // ignoring roots - just update himself
    @Throws(Exception::class)
    private fun update() {
        val start = System.nanoTime()
        try {
            cached = updater()
        } catch(e: Exception){
            onException(name, e)
        }
        log.debug("update '$name': " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds")
    }

    companion object {
        object Invalidator {
            val lock = ReentrantLock()
            val timer = Timer()
            var task: TimerTask? = null
            val agents = HashMap<String, CacheAgent<*>>()

            fun schedule(agent: CacheAgent<*>) {
                lock.lock()
                addAgent(agent)
                if(task == null) {
                    task = createTask()
                    timer.schedule(task, INVALIDATOR_DELAY)
                }
                lock.unlock()
            }

            private fun createTask() = timerTask {
                lock.lock()
                val _agents = agents.values.toList()
                agents.clear()
                task = null
                lock.unlock()

                // here We have graph
                //          0
                //       1     1
                //    2     2     2
                // We can separate levels 0, 1, 2 and run its in parallel
                // But, if we are at level 1, we must wait for the data to update at level 2
                val levelSortedAgents = _agents.sortedByDescending { it.weight }
                var level = levelSortedAgents.firstOrNull()?.weight
                class CacheAgentJob(val job: Job, val agent: CacheAgent<*>)
                val levelJobs = mutableListOf<CacheAgentJob>()

                levelSortedAgents.forEach { agent->
                    with(agent) {
                        runBlocking {
                            mutex.withLock {
                                job?.cancel()
                                if(level != agent.weight) {
                                    // We should get data updated at the nested level before starting update data at this level
                                    // I.e. - wait previous jobs to complete
                                    levelJobs.forEach {
                                        log.debug("wait updating '${it.agent.name}' (level ${it.agent.weight})")
                                        it.job.join()
                                    }
                                    level = agent.weight
                                    levelJobs.clear()
                                }
                                log.debug("schedule updating '$name' (level ${agent.weight})")
                                job = GlobalScope.launch (agentsDispatcher) {
                                    update()
                                }.also { levelJobs.add(CacheAgentJob(it, agent)) }
                            }
                        }
                    }
                }
            }

            // add agent and all roots recursively
            private fun addAgent(agent: CacheAgent<*>) {
                agents[agent.name] = agent
                agent.roots?.forEach {
                    addAgent(it)
                }
            }
        }

        val agentsDispatcher: CoroutineDispatcher by lazy {
            Executors.newFixedThreadPool(MAX_THREADS).asCoroutineDispatcher()
        }
    }
}

typealias Method<R> = ()->R

/** Incorrect order of sequence of calls [LongLivedCache.register] */
class WrongOrderException(m: String) : RuntimeException(m)
class MethodNotRegisteredException(m: String) : RuntimeException(m)
