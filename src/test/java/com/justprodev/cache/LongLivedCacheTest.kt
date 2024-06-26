package com.justprodev.cache

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

const val timeoutInSeconds = 10
const val invalidatorDelay = 1000L
const val maxThreads = 2
const val source = "source"
const val childSource = "child-source"

class LongLivedCacheTest {
    @Test
    fun basic() {
        var value: String?

        val cache = TestCache()

        value = cache.get(source)
        assertEquals(null, value)

        cache.sourceData = "1"
        value = cache.get(source)
        assertEquals("1", value)
        cache.sourceData = "2"
        value = cache.get(source)
        assertEquals("1", value)
    }

    @Test
    fun force() {
        val cache = TestCache()

        // prepare caches
        cache.sourceData = "1"
        cache.get<String>(source)

        cache.sourceData = "2"
        assertEquals("1", cache.get(source))
        assertEquals("2", cache.get(source, forceUpdate = true))
    }

    @Test
    fun invalidate() {
        val cache = TestCache()

        // prepare caches
        cache.sourceData = "1"
        cache.get<String>(source)

        cache.sourceData = "2"
        assertEquals("1", cache.get(source))
        cache.invalidate(source)
        Thread.sleep(invalidatorDelay * 2) // to wait the scheduler with an extra delay
        assertEquals("2", cache.get(source))
        cache.sourceData = "3"
        cache.invalidate(source) {
            assertEquals("3", cache.get(source))
        }
    }

    @Test
    fun invalidateAll() {
        val cache = TestCache()

        // prepare caches
        cache.sourceData = "1"
        cache.get<String>(source)
        cache.childSourceData = "11"
        cache.get<String>(childSource)

        cache.sourceData = "2"
        cache.childSourceData = "22"
        assertEquals("1", cache.get(source))
        assertEquals("11", cache.get(childSource))
        cache.invalidateAll()
        Thread.sleep(invalidatorDelay * 2) // to wait the scheduler with an extra delay
        assertEquals("2", cache.get(source))
        assertEquals("22", cache.get(childSource))
    }

    @Test
    fun invalidateRootByChild() {
        val cache = TestCache()

        // prepare caches
        cache.sourceData = "1"
        cache.get<String>(source)
        cache.childSourceData = "11"
        cache.get<String>(childSource)

        cache.sourceData = "2"
        cache.invalidate(childSource)
        assertEquals("1", cache.get(source), "cached value should be updated with the invalidator delay = $invalidatorDelay")
        Thread.sleep(invalidatorDelay * 2) // to wait the scheduler with an extra delay
        assertEquals("2", cache.get(source))
    }

    @Test
    fun invalidateRootByChildWithForceGet() {
        val cache = TestCache()

        // prepare caches
        cache.sourceData = "1"
        cache.get<String>(source)
        cache.childSourceData = "11"
        cache.get<String>(childSource)

        cache.sourceData = "2"
        cache.get<String>(childSource, forceUpdate = true)
        assertEquals("1", cache.get(source), "cached value should be updated with the invalidator delay = $invalidatorDelay")
        Thread.sleep(invalidatorDelay * 2) // to wait the scheduler with an extra delay
        assertEquals("2", cache.get(source))
    }

    @Test
    fun isRegistered() {
        val cache = TestCache()

        assertEquals(true, cache.isRegistered(source))
        assertEquals(true, cache.isRegistered(childSource))
        assertEquals(false, cache.isRegistered("unknown"))
    }

    @Test
    fun unregister() {
        val cache = TestCache()

        assertEquals(true, cache.isRegistered(source))
        assertEquals(true, cache.isRegistered(childSource))
        cache.unregister(childSource)
        assertEquals(false, cache.isRegistered(source))
        assertEquals(false, cache.isRegistered(childSource))
    }
}

class TestCache : LongLivedCache(timeoutInSeconds, invalidatorDelay = invalidatorDelay, maxThreads = maxThreads) {
    init {
        register(source, { sourceData?.let { String(it.toByteArray()) } })
        register(childSource, { childSourceData?.let { String(it.toByteArray()) } }, listOf(source))
    }

    var sourceData: String? = null
    var childSourceData: String? = null
}

