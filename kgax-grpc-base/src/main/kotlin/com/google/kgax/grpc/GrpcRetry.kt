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

package com.google.kgax.grpc

import com.google.kgax.Retry
import com.google.kgax.RetryContext
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.math.pow

/**
 * A gRPC status code based retry policy.
 *
 * A method will be retried when an entry in the [filter] matches the error's status code.
 *
 * For example, given the following filter:
 *
 * ```
 * val filter = mapOf("a" to setOf(Status.UNAVAILABLE))
 * ```
 *
 * methods will be retried whenever a retry context has an id of "a" and the error
 * is a gRPC StatusRuntimeException with status code Status.UNAVAILABLE. Otherwise, no
 * retry will be attempted.
 */
abstract class GrpcRetry(private val filter: Map<String, Set<Status.Code>>) : Retry {

    /** Called when the filter matches the error's status code */
    abstract fun retryAfterStatus(error: StatusRuntimeException, context: RetryContext): Long?

    final override fun retryAfter(error: Throwable, context: RetryContext): Long? {
        val statusError = error as? StatusRuntimeException
        val retryOn = filter[context.id]
        if (retryOn != null && statusError != null) {
            if (retryOn.contains(statusError.status.code)) {
                return retryAfterStatus(statusError, context)
            }
        }

        // do not retry by default
        return null
    }
}

/**
 * A basic backoff retry policy that retries on the given status codes.
 *
 * Retries occur after and initial delay that grows over time until the maximum number of
 * attempts have been made.
 */
open class GrpcBasicRetry(
    filter: Map<String, Set<Status.Code>>,
    private val initialDelay: Long = 100,
    private val multiplier: Double = 1.3,
    private val maxAttempts: Int = 10
) : GrpcRetry(filter) {

    override fun retryAfterStatus(error: StatusRuntimeException, context: RetryContext): Long? {
        if (context.numberOfAttempts >= maxAttempts) return null
        return (initialDelay * multiplier.pow(context.numberOfAttempts)).toLong()
    }
}