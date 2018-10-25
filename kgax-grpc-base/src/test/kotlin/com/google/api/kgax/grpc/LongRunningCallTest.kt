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

package com.google.api.kgax.grpc

import com.google.common.truth.Truth.assertThat
import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.SettableFuture
import com.google.longrunning.Operation
import com.google.longrunning.OperationsGrpc
import com.google.protobuf.Any
import com.google.protobuf.StringValue
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import io.grpc.CallOptions
import io.grpc.Status
import kotlin.test.Test
import kotlin.test.fail

class LongRunningCallTest {

    @Test
    fun `LRO waits until done`() {
        val lroResponse = StringValue.newBuilder().setValue("long time").build()

        val futureDoneOk = SettableFuture.create<Operation>()
        futureDoneOk.set(
            Operation.newBuilder()
                .setName("b")
                .setResponse(
                    Any.newBuilder()
                        .setValue(lroResponse.toByteString())
                        .setTypeUrl("google.protobuf.StringValue")
                        .build()
                )
                .setDone(true)
                .build()
        )
        val futureDoneError = SettableFuture.create<Operation>()
        futureDoneError.set(
            Operation.newBuilder()
                .setName("b")
                .setError(
                    com.google.rpc.Status.newBuilder()
                        .setCode(Status.UNKNOWN.code.value())
                        .build()
                )
                .setDone(true)
                .build()
        )

        for (futureDone in listOf(futureDoneOk, futureDoneError)) {
            val future1 = SettableFuture.create<Operation>()
            future1.set(
                Operation.newBuilder()
                    .setName("a")
                    .setDone(false)
                    .build()
            )

            val callContext = ClientCallContext()
            val callOptions: CallOptions = mock {
                on { getOption(eq(ClientCallContext.KEY)) } doReturn callContext
            }
            val opStub: OperationsGrpc.OperationsFutureStub = mock {
                on { getOperation(any()) } doReturn future1
                on { getOperation(any()) } doReturn futureDone
            }
            whenever(opStub.withInterceptors(any())).doReturn(opStub)
            whenever(opStub.withOption(any<CallOptions.Key<*>>(), any())).doReturn(opStub)
            whenever(opStub.callOptions).doReturn(callOptions)

            val grpcClient =
                GrpcClientStub(opStub, ClientCallOptions())

            val future = SettableFuture.create<CallResult<Operation>>()
            future.set(
                CallResult(
                    Operation.newBuilder()
                        .setName("test_op")
                        .setDone(false)
                        .build(), mock()
                )
            )
            val lro = LongRunningCall(grpcClient, future, StringValue::class.java)

            if (futureDone == futureDoneOk) {
                val result = lro.waitUntilDone()
                assertThat(result.body).isInstanceOf(StringValue::class.java)
                assertThat(result.body.value).isEqualTo("long time")
            } else {
                var error: Throwable? = null
                try {
                    lro.waitUntilDone()
                } catch (ex: Throwable) {
                    error = ex
                }
                assertThat(error).isNotNull()
            }
        }
    }

    @Test
    fun `LRO behaves as a future`() {
        val operationFuture = SettableFuture.create<CallResult<Operation>>()
        val operation = Operation.newBuilder().setDone(true).build()
        operationFuture.set(CallResult(operation, ResponseMetadata()))
        val grpcClient: GrpcClientStub<OperationsGrpc.OperationsFutureStub> = mock {
            on { executeFuture<Operation>(any(), any()) }
                .thenReturn(operationFuture)
        }

        val lro = LongRunningCall(grpcClient, operationFuture, StringValue::class.java)
        lro.get()

        assertThat(lro.isDone).isTrue()
        assertThat(lro.operation).isEqualTo(operation)
    }

    @Test
    fun `LRO behaves as a future on`() {
        val operationFuture = SettableFuture.create<CallResult<Operation>>()
        val operation = Operation.newBuilder().setDone(true).build()
        operationFuture.set(CallResult(operation, ResponseMetadata()))
        val grpcClient: GrpcClientStub<OperationsGrpc.OperationsFutureStub> = mock {
            on { executeFuture<Operation>(any(), any()) }
                .thenReturn(operationFuture)
        }

        val lro = LongRunningCall(grpcClient, operationFuture, StringValue::class.java)
        lro.on(MoreExecutors.directExecutor()) {
            success = { assertThat(it.body.value).isEqualTo("") }
            error = { fail("error not expected") }
        }
        assertThat(lro.isDone).isTrue()
        assertThat(lro.operation).isEqualTo(operation)
    }
}
