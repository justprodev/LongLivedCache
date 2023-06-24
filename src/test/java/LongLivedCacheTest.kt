import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import kz.technodom.inbody.util.LongLivedCache
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import kotlin.test.assertEquals

const val timeoutInSeconds = 10
const val invalidatorDelay = 1000L
const val maxThreads = 2
val source = "source"
const val childSource = "child-source"

class LongLivedCacheTest {
    private var logWatcher: ListAppender<ILoggingEvent>? = null

    @BeforeEach
    fun setup() {
        logWatcher = ListAppender()
        logWatcher!!.start()
        (LoggerFactory.getLogger(TestCache::class.java) as Logger).addAppender(logWatcher)
    }

    @AfterEach
    fun teardown() {
        (LoggerFactory.getLogger(TestCache::class.java) as Logger).detachAndStopAllAppenders()
    }

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
        cache.get<String>(childSource, forceUpdate = true)
        assertEquals("1", cache.get(source), "cached value should be updated with the invalidator delay = $invalidatorDelay")
        Thread.sleep(invalidatorDelay * 2) // to wait the scheduler with an extra delay
        assertEquals("2", cache.get(source))
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

