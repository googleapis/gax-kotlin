/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.kgax

/** Retry policy for API calls that result in an exception. */
interface Retry {
    /**
     * Retry after the given number of milliseconds on the [error].
     * Do not retry if the return value is null.
     */
    fun retryAfter(error: Throwable, context: RetryContext): Long?
}

/** Context of a retry. */
data class RetryContext(
    val id: String,
    val initialRequestTime: Long = System.currentTimeMillis(),
    val numberOfAttempts: Int = 0
) {
    /** Get a context for the next attempt */
    fun next() = RetryContext(id, initialRequestTime, numberOfAttempts + 1)
}

/** Never retry. */
object NoRetry : Retry {
    override fun retryAfter(error: Throwable, context: RetryContext): Long? = null
}
