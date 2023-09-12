package com.justprodev.cache

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.concurrent.timer

/**
 * The "long-lived" cache for a some heavily loaded but few updatable services
 *
 * Cache entities can be connected by relation child->parents.
 * This connection guarantees that parent always be updated if child is wanted to be updated.
 *
 * All configuration should be made via [register]
 *
 * @param invalidatorTimeoutInSeconds in seconds, after that [invalidateAll] will be fired automatically
 * @param maxThreads maximum threads for parallel updating cache entities
 *
 * @author alex@justprodev.com
 */
open class LongLivedCache(
    invalidatorTimeoutInSeconds: Int,
    maxThreads: Int = 3,
    invalidatorDelay: Long = 1000L
) {
    private val invalidator = CacheInvalidator(invalidatorDelay, maxThreads)
    private val agents = HashMap<String, CacheAgent<*>>()
    private val logger = LoggerFactory.getLogger(this::class.java)

    init {
        val ttl = TimeUnit.SECONDS.toMillis(invalidatorTimeoutInSeconds.toLong())
        timer("${this.javaClass.name}_invalidate", initialDelay = ttl, period = ttl) {
            logger.debug("invalidate ${agents.size} agents by timeout ($invalidatorTimeoutInSeconds seconds)")
            invalidateAll()
        }
        logger.debug("init: threads = $maxThreads, timeout = $ttl ms, invalidator_delay = $invalidatorDelay ms")
    }

    /**
     * @param name should be registered with [register] before
     * @param forceUpdate update cached value and return
     *
     * @throws MethodNotRegisteredException if [name] isn't registered
     */
    @Suppress("UNCHECKED_CAST")
    @Throws(MethodNotRegisteredException::class, UpdaterException::class)
    fun <R> get(name: String, forceUpdate: Boolean = false): R {
        val agent = agents[name] ?: throw MethodNotRegisteredException("Method $name not registered")

        val r = if (forceUpdate) {
            agent.update().also {
                agent.roots?.forEach { invalidator.invalidate(it) }
            }
        } else {
            agent.get()
        }

        return r as R
    }

    /**
     * @param name should be registered with [register] before
     * @param onFinish signaling that invalidating was finished
     */
    @JvmOverloads
    @Throws(MethodNotRegisteredException::class)
    fun invalidate(name: String, onFinish: (() -> Unit)? = null) {
        val agent = agents[name] ?: throw MethodNotRegisteredException("Method $name not registered")
        invalidator.invalidate(agent, onFinish)
    }

    fun invalidateAll() {
        agents.keys.forEach { invalidate(it) }
    }

    /**
     * Register new cache entity with [name]
     */
    @Throws(WrongOrderException::class)
    fun <R> register(
        name: String,
        method: () -> R,
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
     * Check if cache entity [name] is registered
     */
    fun isRegistered(name: String) = agents.containsKey(name)

    /**
     * Unregister cache entity [name] and all related agents recursively
     */
    fun unregister(name: String) {
        val agent = agents[name] ?: return

        agent.roots?.forEach {
            unregister(it.name)
        }

        logger.info("unregister agent '$name'")
        agents.remove(name)
    }

    /**
     * Create and register new [CacheAgent] with [name]
     *
     * @param name for the new agent
     * @param roots agents to upgrade before upgrading agent
     * @return create agent
     */
    private fun <R> _register(
        name: String,
        method: () -> R,
        roots: List<CacheAgent<*>>? = null
    ): CacheAgent<R> {
        logger.info("register agent '$name'")
        return CacheAgent(name, method, roots).also {
            agents[name] = it
        }
    }
}

/** Incorrect order of sequence of calls [LongLivedCache.register] */
class WrongOrderException(m: String) : RuntimeException(m)
class MethodNotRegisteredException(m: String) : RuntimeException(m)
