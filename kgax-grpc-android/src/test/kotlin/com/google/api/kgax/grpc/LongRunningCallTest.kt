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

import com.google.api.kgax.Retry
import com.google.api.kgax.RetryContext
import com.google.common.truth.Truth.assertThat
import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.SettableFuture
import com.google.longrunning.Operation
import com.google.longrunning.OperationsClientStub
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.StringValue
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.reset
import com.nhaarman.mockito_kotlin.whenever
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.Status
import io.grpc.stub.AbstractStub
import java.util.concurrent.ExecutionException
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.fail

class LongRunningCallTest2 {

    private val channel: Channel = mock()
    private val clientCall: ClientCall<*, *> = mock()
    private val callOptions: CallOptions = mock()
    private val responseMetadata: ResponseMetadata = mock()
    private val callContext: ClientCallContext = mock()

    @BeforeTest
    fun before() {
        reset(channel, clientCall, callOptions, responseMetadata, callContext)
    }

    @Test
    fun `LRO waits until done`() {
        val futureDoneOk = SettableFuture.create<Operation>()
        futureDoneOk.set(
            Operation.newBuilder()
                .setName("b")
                .setResponse(
                    Any.newBuilder()
                        .setValue(StringValue.newBuilder().setValue("long time").build().toByteString())
                        .setTypeUrl("type.googleapis.com/google.protobuf.StringValue")
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
            val opStub: OperationsClientStub = mock {
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
                        .build(),
                    mock()
                )
            )
            val lro = LongRunningCall(grpcClient, future, StringValue::class.java)

            if (futureDone == futureDoneOk) {
                val result = lro.asFuture().get()
                assertThat(result.body.value).isEqualTo("long time")
            } else {
                var error: Throwable? = null
                try {
                    lro.asFuture().get()
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
        val operation = Operation.newBuilder()
            .setDone(true)
            .setResponse(Any.newBuilder()
                .setValue(ByteString.copyFromUtf8(""))
                .build())
            .build()
        operationFuture.set(CallResult(operation, ResponseMetadata()))
        val grpcClient: GrpcClientStub<OperationsClientStub> = mock {
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
        val operation = Operation.newBuilder()
            .setDone(true)
            .setResponse(Any.newBuilder()
                .setValue(ByteString.copyFromUtf8(""))
                .build())
            .build()
        operationFuture.set(CallResult(operation, ResponseMetadata()))
        val grpcClient: GrpcClientStub<OperationsClientStub> = mock {
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

    @Test
    fun `Can do a long running call`() {
        val stub: TestStub = createTestStubMock()
        val operation = Operation.newBuilder()
            .setName("the op")
            .setDone(true)
            .setResponse(Any.newBuilder()
                .setValue(ByteString.copyFromUtf8(""))
                .build())
            .build()

        val call = GrpcClientStub(stub, ClientCallOptions())
        val result = call.executeLongRunning(StringValue::class.java) { arg ->
            assertThat(arg).isEqualTo(stub)
            val operationFuture = SettableFuture.create<Operation>()
            operationFuture.set(operation)
            operationFuture
        }
        result.get()
        assertThat(result.operation).isEqualTo(operation)
    }

    @Test
    fun `Can retry a long running call`() {
        val stub: TestStub = createTestStubMock()
        val exception = IllegalArgumentException("bad lro")
        val operation = Operation.newBuilder()
            .setName("the op")
            .setDone(true)
            .setResponse(Any.newBuilder()
                .setValue(ByteString.copyFromUtf8(""))
                .build())
            .build()

        val errorFuture = SettableFuture.create<Operation>()
        errorFuture.setException(exception)

        val retry = object : Retry {
            var executed = false

            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                assertThat(error).isEqualTo(exception)
                assertThat(context.numberOfAttempts).isEqualTo(0)
                executed = true
                return 400
            }
        }

        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))
        val result = call.executeLongRunning(StringValue::class.java) { arg ->
            assertThat(arg).isEqualTo(stub)
            val operationFuture = SettableFuture.create<Operation>()
            operationFuture.set(operation)
            if (!retry.executed) {
                errorFuture
            } else {
                operationFuture
            }
        }
        result.get()

        assertThat(result.operation).isEqualTo(operation)
        assertThat(retry.executed).isTrue()
    }

    @Test(expected = ExecutionException::class)
    fun `Throws on an invalid null long running call`() {
        val stub: TestStub = createTestStubMock()

        val call = GrpcClientStub(stub, ClientCallOptions())
        call.executeLongRunning(StringValue::class.java) {
            val operationFuture = SettableFuture.create<Operation>()
            operationFuture.set(null)
            operationFuture
        }.get()
    }

    private fun createTestStubMock(): TestStub {
        val stub: TestStub = mock()
        whenever(stub.channel).thenReturn(channel)
        whenever(stub.withInterceptors(any())).thenReturn(stub)
        whenever(stub.withCallCredentials(any())).thenReturn(stub)
        whenever(stub.withOption(any(), any<kotlin.Any>())).thenReturn(stub)
        whenever(stub.callOptions).thenReturn(callOptions)
        whenever(callOptions.getOption(eq(ClientCallContext.KEY))).thenReturn(callContext)
        whenever(callContext.call).thenReturn(clientCall)
        whenever(callContext.responseMetadata).thenReturn(responseMetadata)
        return stub
    }

    private class TestStub(channel: Channel, options: CallOptions) :
        AbstractStub<TestStub>(channel, options) {

        override fun build(channel: Channel, options: CallOptions): TestStub {
            return TestStub(channel, options)
        }
    }
}
