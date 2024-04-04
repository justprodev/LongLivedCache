package com.justprodev.cache

import com.justprodev.cache.update.ScheduledUpdate
import com.justprodev.cache.update.Scheduler
import org.slf4j.Logger

/**
 * Shadow updating the cache entities in parallel
 *
 * @param period - period between updating scheduled agents
 * @param maxThreads - maximum threads for parallel updating cache entities
 * @param logger - logger for debug information
 */
internal class CacheInvalidator(
    period: Long,
    maxThreads: Int,
    logger: Logger,
) {
    private val scheduler = Scheduler(period, maxThreads, logger)

    /**
     * Schedules an updating of the [agent]
     *
     * Also, schedules updating of all roots of the [agent] recursively
     *
     * @param agent to update
     * @param onFinish optional callback that will be called after updating
     */
    fun invalidate(agent: CacheAgent<*>, onFinish: (() -> Unit)? = null) {
        val updates = arrayListOf<ScheduledUpdate>().apply {
            // add agent and all roots recursively
            fun addAgent(agent: CacheAgent<*>, onFinish: (() -> Unit)? = null) {
                add(ScheduledUpdate(agent, onFinish))
                agent.roots?.forEach(::addAgent)
            }

            addAgent(agent, onFinish)
        }
        scheduler.add(updates)
    }
}

