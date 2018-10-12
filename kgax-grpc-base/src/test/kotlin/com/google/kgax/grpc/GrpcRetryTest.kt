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

import com.google.common.truth.Truth.assertThat
import com.google.kgax.RetryContext
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.lang.RuntimeException
import kotlin.test.Test

class GrpcRetryTest {

    private val errorAborted = StatusRuntimeException(Status.ABORTED)
    private val errorDataLoss = StatusRuntimeException(Status.DATA_LOSS)
    private val errorUnknown = StatusRuntimeException(Status.UNKNOWN)

    @Test
    fun `Retry can get next`() {
        var context = RetryContext("some_thing")
        val id = context.id
        val time = context.initialRequestTime
        assertThat(context.numberOfAttempts).isEqualTo(0)

        context = context.next()
        assertThat(context.numberOfAttempts).isEqualTo(1)
        assertThat(context.id).isEqualTo(id)
        assertThat(context.initialRequestTime).isEqualTo(time)

        context = context.next()
        assertThat(context.numberOfAttempts).isEqualTo(2)
        assertThat(context.id).isEqualTo(id)
        assertThat(context.initialRequestTime).isEqualTo(time)
    }

    @Test
    fun `Can retry on status`() {
        val retry = GrpcBasicRetry(
            mapOf(
                "one" to setOf(Status.Code.ABORTED, Status.Code.DATA_LOSS),
                "two" to setOf()
            ), initialDelay = 150
        )

        assertThat(retry.retryAfter(errorAborted, RetryContext("one"))).isEqualTo(150)
        assertThat(retry.retryAfter(errorAborted, RetryContext("one", numberOfAttempts = 5))).isEqualTo(556)
        assertThat(retry.retryAfter(errorAborted, RetryContext("two"))).isNull()
        assertThat(retry.retryAfter(errorAborted, RetryContext("ONE"))).isNull()

        assertThat(retry.retryAfter(errorDataLoss, RetryContext("one"))).isEqualTo(150)
        assertThat(retry.retryAfter(errorDataLoss, RetryContext("one", numberOfAttempts = 5))).isEqualTo(556)
        assertThat(retry.retryAfter(errorDataLoss, RetryContext("two"))).isNull()

        assertThat(retry.retryAfter(errorUnknown, RetryContext("one"))).isNull()
        assertThat(retry.retryAfter(errorUnknown, RetryContext("two"))).isNull()
        assertThat(retry.retryAfter(errorUnknown, RetryContext("ONE"))).isNull()
    }

    @Test
    fun `Can stop retrying`() {
        val retry = GrpcBasicRetry(
            mapOf(
                "one" to setOf(Status.Code.ABORTED, Status.Code.DATA_LOSS)
            ), maxAttempts = 20
        )

        assertThat(retry.retryAfter(errorAborted, RetryContext("one"))).isNotNull()
        for (i in 0..19) {
            assertThat(retry.retryAfter(errorAborted, RetryContext("one", numberOfAttempts = i))).isNotNull()
        }
        for (i in 20..100) {
            assertThat(retry.retryAfter(errorAborted, RetryContext("one", numberOfAttempts = i))).isNull()
        }
    }

    @Test
    fun `Ignores other exceptions`() {
        val retry = GrpcBasicRetry(
            mapOf(
                "one" to setOf(Status.Code.ABORTED, Status.Code.DATA_LOSS)
            )
        )

        assertThat(retry.retryAfter(RuntimeException(), RetryContext(""))).isNull()
    }
}
