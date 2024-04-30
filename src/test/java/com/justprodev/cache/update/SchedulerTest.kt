package com.justprodev.cache.update

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import org.slf4j.LoggerFactory

const val PERIOD = 1000L
// should at least 2 threads to test parallel processing
const val MAX_THREADS = 3

class SchedulerTest {
    private val logger = LoggerFactory.getLogger(SchedulerTest::class.java)
    private lateinit var scheduler: Scheduler

    @BeforeEach
    fun setUp() {
        scheduler = Scheduler(PERIOD, MAX_THREADS, logger)
    }

    @Test
    fun oneLevelUpdate() {
        var counter = 0
        val updates = listOf(
            ScheduledUpdate("1", 1, { counter++ }),
            ScheduledUpdate("2", 1, { counter++ }),
            ScheduledUpdate("3", 1, { counter++ }),
        )
        scheduler.add(updates)
        Thread.sleep(PERIOD * 2)
        assertEquals(3, counter, "All updates should be processed (${counter} processed)")
    }

    @Test
    fun onFinish() {
        var counter = 0
        scheduler.add(listOf(ScheduledUpdate("1", 1, {}, { counter++ })))
        Thread.sleep(PERIOD * 2)
        assertEquals(1, counter, "perhaps onFinish is not called")
    }

    @Test
    fun multiLevelUpdate() {
        var counter = 0
        val updates = listOf(
            ScheduledUpdate(
                "1",
                1,
                { counter++ },
                { assertEquals(counter, 3, "update should be processed third") }
            ),
            ScheduledUpdate(
                "2",
                2,
                { counter++ },
                { assertEquals(counter, 2, "update should be processed second") }
            ),
            ScheduledUpdate(
                "3",
                3,
                { counter++ },
                { assertEquals(counter, 1, "update should be processed first") }
            ),
        )
        scheduler.add(updates)
        Thread.sleep(PERIOD * 2)
        assertEquals(3, counter, "All updates should be processed (${counter} processed)")
    }

    @Test
    fun multiLevelWithManyEntriesUpdate() {
        var counter = 0
        lateinit var level2ThreadName1: String
        lateinit var level2ThreadName2: String

        val updates = listOf(
            ScheduledUpdate(
                "1",
                1,
                { counter++ },
                { assertEquals(counter, 3, "update should be processed second") }
            ),
            ScheduledUpdate(
                "2_1",
                2,
                { counter++; level2ThreadName1 = Thread.currentThread().name }
            ),
            ScheduledUpdate(
                "2_2",
                2,
                { counter++; level2ThreadName2 = Thread.currentThread().name }
            ),
        )
        scheduler.add(updates)
        Thread.sleep(PERIOD * 2)
        assertEquals(3, counter, "All updates should be processed (${counter} processed)")
        assertNotEquals(level2ThreadName1, level2ThreadName2, "level 2 updates should be processed in parallel")
    }

    @Test
    fun errorInUpdate() {
        var counter = 0
        val updates = listOf(
            ScheduledUpdate("1", 1, { counter++ }),
            ScheduledUpdate("2", 1, {
                Thread.sleep(500)
                error("\"this error was generated specifically" +
                        " for the test:level 2 updates should be processed in parallel, don't panic: " +
                        "error in update 2 level")
            }),
        )
        scheduler.add(updates)
        Thread.sleep(PERIOD * 2)
        assertEquals(1, counter, "first level should be updated")
    }
}