package com.justprodev.cache

import java.lang.ref.SoftReference
import java.util.*

/**
 * An agent that retrieves result from [updater].
 *
 * @param name unique name of the agent
 * @param R result of the [updater]
 * @param updater function that produces some result that will be cached
 * @param roots related [CacheAgent]'s that will be updated by this agent after updating himself - it's guaranteed
 */
internal class CacheAgent<R>(
    val name: String,
    private val updater: () -> R,
    val roots: List<CacheAgent<*>>? = null
) {
    private var value: SoftReference<R>? = null

    /**
     * level, 0 - no roots, 1 - roots without other roots, ...
     */
    val level by lazy {
        fun calculateLevel(prevLevel: Int, agent: CacheAgent<*>): Int {
            return agent.roots?.let { roots ->
                roots.maxOf { root -> calculateLevel(prevLevel + 1, root) }
            } ?: prevLevel
        }
        calculateLevel(0, this)
    }

    /**
     * Get the value.
     *
     * @throws Throwable if some errors during the call [updater]
     */
    @Throws(UpdaterException::class)
    fun get(): R = value?.get() ?: update()

    /**
     * Update the cached value.
     *
     * @throws Throwable if some errors during the call [updater]
     */
    @Throws(UpdaterException::class)
    @Synchronized
    fun update(): R {
        val newValue = updater()
        value = SoftReference(newValue)
        return newValue
    }
}

/** Exception during updating cache entity */
class UpdaterException(cause: Throwable) : RuntimeException(cause)