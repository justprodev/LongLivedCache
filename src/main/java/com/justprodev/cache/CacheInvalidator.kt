package com.justprodev.cache

import kotlinx.coroutines.*
import org.slf4j.Logger
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.timerTask

/**
 * Shadow updating the cache entities in parallel
 *
 * [invalidatorDelay] - delay before invalidating cache entities, to group them
 */
internal class CacheInvalidator(
    private val invalidatorDelay: Long,
    maxThreads: Int,
    private val logger: Logger
) {
    private val agentsDispatcher = Executors.newFixedThreadPool(maxThreads).asCoroutineDispatcher()
    private val lock = ReentrantLock()
    private val timer = Timer()
    private var task: TimerTask? = null
    private val agents = HashMap<String, AgentToInvalidate>()

    private class AgentToInvalidate(val agent: CacheAgent<*>, val onFinish: (() -> Unit)? = null)

    /**
     * Invalidates agent and its roots
     */
    fun invalidate(agent: CacheAgent<*>, onFinish: (() -> Unit)? = null) = with(agent) {
        logger.debug("invalidate '$name'")
        schedule(this, onFinish)
    }

    private fun schedule(agent: CacheAgent<*>, onFinish: (() -> Unit)?) {
        lock.lock()
        addAgent(agent, onFinish)
        if (task == null) {
            task = createTask()
            timer.schedule(task, invalidatorDelay)
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
        val levelSortedAgents = _agents.sortedByDescending { it.agent.level }
        var level = levelSortedAgents.firstOrNull()?.agent?.level

        class CacheAgentJob(val job: Job, val agent: CacheAgent<*>)

        val levelJobs = mutableListOf<CacheAgentJob>()

        runBlocking {
            levelSortedAgents.forEach { agentToInvalidate ->
                val agent = agentToInvalidate.agent
                val onFinish = agentToInvalidate.onFinish

                with(agent) {
                    if (level != this.level) {
                        // We should get data updated at the nested level before starting update data at this level
                        // I.e. - wait previous jobs to complete
                        levelJobs.forEach {
                            logger.debug("wait update '${it.agent.name}' (level ${it.agent.level})")
                            it.job.join()
                        }
                        level = this.level
                        levelJobs.clear()
                    }
                    logger.debug("schedule update '$name' (level ${this.level})")
                    val job = GlobalScope.launch(agentsDispatcher) {
                        update()
                        logger.debug("updated '$name' (level ${this@with.level})")
                        onFinish?.invoke()
                    }
                    levelJobs.add(CacheAgentJob(job, this))
                }
            }
        }
    }

    // add agent and all roots recursively
    private fun addAgent(agent: CacheAgent<*>, onFinish: (() -> Unit)? = null) {
        agents[agent.name] = AgentToInvalidate(agent, onFinish)
        agent.roots?.forEach {
            addAgent(it)
        }
    }
}