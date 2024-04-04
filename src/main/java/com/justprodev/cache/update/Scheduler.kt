package com.justprodev.cache.update

import com.justprodev.cache.CacheAgent
import kotlinx.coroutines.*
import org.slf4j.Logger
import java.util.*
import java.util.concurrent.Executors
import kotlin.concurrent.scheduleAtFixedRate

/**
 * Shadow updating the cache entities in parallel.
 *
 * Use [add] to add a batch of scheduled updates.
 *
 * @param period - period between updating scheduled agents
 * @param maxThreads - maximum threads for parallel updating cache entities
 * @param logger - logger for debug information
 */
internal class Scheduler(
    period: Long,
    maxThreads: Int,
    private val logger: Logger,
) {
    private val schedulerScope = CoroutineScope(Executors.newFixedThreadPool(maxThreads).asCoroutineDispatcher())
    private val waitingUpdates = HashMap<String, ScheduledUpdate>()

    init {
        // start daemon timer with one periodical task to process updates every [delay] milliseconds
        Timer(true).apply {
            scheduleAtFixedRate(period, period) {
                val batch = getBatch()
                if (batch.isNotEmpty()) {
                    logger.info("start process batch of ${batch.size} updates")
                    process(batch)
                    logger.info("end process batch of ${batch.size} updates")
                }
            }
        }
    }

    /**
     * Add a batch of scheduled updates
     */
    @Synchronized
    fun add(updates: Collection<ScheduledUpdate>) {
        updates.forEach { update ->
            logger.debug("schedule updating {}", update)
            // just replace the update if it already exists
            waitingUpdates[update.name] = update
        }
    }

    /**
     * Process a batch of updates
     *
     * Function is blocking until processing of [updates] is finished
     *
     * @param updates - batch of updates
     */
    private fun process(updates: Collection<ScheduledUpdate>) {
        // Initially we have graph
        //          0
        //       1     1
        //    2     2     2
        // We can separate levels 0, 1, 2 and run its in parallel
        // But, if we are at level 1, we must wait for the data to update at level 2, and so on
        // sort updates by level in descending order
        val levelSortedUpdates = updates.sortedByDescending { it.level }
        // start from the highest level
        var currentLevel = levelSortedUpdates.firstOrNull()?.level

        class UpdatingJob(val job: Job, val update: ScheduledUpdate)

        val levelJobs = mutableListOf<UpdatingJob>()

        runBlocking {
            levelSortedUpdates.forEach { update ->
                val onFinish = update.onFinish

                if (update.level != currentLevel) {
                    // wait jobs at previous level
                    levelJobs.forEach {
                        logger.debug("wait coroutine job to update {}", it.update)
                        it.job.join()
                    }
                    currentLevel = update.level
                    levelJobs.clear()
                }
                logger.debug("create coroutine job to update {}", update)
                val job = schedulerScope.launch {
                    update.func()
                    logger.debug("updated {}", update)
                    onFinish?.invoke()
                }
                levelJobs.add(UpdatingJob(job, update))
            }
            // wait all jobs at the last level
            levelJobs.forEach {
                logger.debug("wait coroutine job to update {}", it.update)
                it.job.join()
            }
        }
    }

    /**
     * Copies ALL values from [waitingUpdates] and clears it
     *
     * @return batch of updates
     */
    @Synchronized
    private fun getBatch(): Collection<ScheduledUpdate> {
        val batch = waitingUpdates.values.toList()
        waitingUpdates.clear()
        return batch
    }
}

/**
 * Scheduled update of the agent
 *
 * @param name unique name of the agent
 * @param level level of the agent
 * @param func function to update the agent
 * @param onFinish optional callback that will be called after updating
 */
internal class ScheduledUpdate(
    val name: String,
    val level: Int,
    val func: () -> Unit,
    val onFinish: (() -> Unit)?,
) {
    /**
     * Create a scheduled update from the [agent]
     */
    constructor(agent: CacheAgent<*>, onFinish: (() -> Unit)?) : this(
        agent.name, agent.level, agent::update, onFinish
    )

    override fun toString() = "name=$name, level=$level"
}