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
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.SettableFuture
import com.google.api.kgax.Retry
import com.google.api.kgax.RetryContext
import com.google.longrunning.Operation
import com.google.protobuf.Int32Value
import com.google.protobuf.StringValue
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.check
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.never
import com.nhaarman.mockito_kotlin.reset
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import io.grpc.CallCredentials
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.stub.AbstractStub
import io.grpc.stub.StreamObserver
import java.io.ByteArrayInputStream
import java.io.IOException
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executor
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.fail

class GrpcClientStubTest {

    fun StringValue(value: String): StringValue = StringValue.newBuilder().setValue(value).build()
    fun Int32Value(value: Int): Int32Value = Int32Value.newBuilder().setValue(value).build()

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
    fun `ClientCallOptions remembers metadata`() {
        val options = ClientCallOptions.Builder()
        options.withMetadata("foo", listOf("a", "b"))
        options.withMetadata("bar", listOf("1", "2"))
        options.withMetadata("foo", listOf("one"))

        assertThat(options.requestMetadata).containsExactlyEntriesIn(
            mapOf("foo" to listOf("one"), "bar" to listOf("1", "2"))
        )
    }

    @Test
    fun `ClientCallOptions forgets metadata`() {
        val options = ClientCallOptions.Builder()
        options.withMetadata("foo", listOf("a", "b"))
        options.withMetadata("bar", listOf("1", "2"))
        options.withoutMetadata("foo")

        assertThat(options.requestMetadata).containsExactlyEntriesIn(
            mapOf("bar" to listOf("1", "2"))
        )
    }

    @Test
    fun `ClientCallOptions can be built from an existing option`() {
        val interceptor1: ClientInterceptor = mock()
        val interceptor2: ClientInterceptor = mock()
        val retry: Retry = mock()

        val opts = clientCallOptions {
            withAccessToken(mock(), listOf("scope"))
            withInterceptor(interceptor1)
            withInterceptor(interceptor2)
            withInitialRequest(StringValue("!"))
            withMetadata("a", listOf("aa", "aaa"))
            withMetadata("b", listOf("bb"))
            withRetry(retry)
        }

        val newOpts = ClientCallOptions.Builder(opts).build()

        assertThat(opts).isNotEqualTo(newOpts)
        assertThat(newOpts.interceptors).containsExactly(interceptor1, interceptor2)
        assertThat(newOpts.initialRequests).containsExactly(StringValue("!"))
        assertThat(newOpts.credentials).isNotNull()
        assertThat(newOpts.requestMetadata.keys).containsExactly("a", "b")
        assertThat(newOpts.retry).isEqualTo(retry)
    }

    @Test
    fun `ClientCallOptions can drop metadata`() {
        val opts = clientCallOptions {
            withAccessToken(mock())
            withMetadata("a", listOf("aa", "aaa"))
            withMetadata("b", listOf("bb"))
            withMetadata("c", listOf("go away"))
            withoutMetadata("c")
            withoutMetadata("a")
        }

        val newOpts = ClientCallOptions.Builder(opts).build()

        assertThat(opts).isNotEqualTo(newOpts)
        assertThat(newOpts.interceptors).isEmpty()
        assertThat(newOpts.initialRequests).isEmpty()
        assertThat(newOpts.credentials).isNotNull()
        assertThat(newOpts.requestMetadata.keys).containsExactly("b")
        assertThat(newOpts.requestMetadata["b"]).containsExactly("bb")
    }

    @Test(expected = IOException::class)
    fun `ClientCallOptions can use credentials`() {
        clientCallOptions {
            ByteArrayInputStream("{}".toByteArray()).use {
                withServiceAccountCredentials(it)
            }
        }
    }

    @Test(expected = IOException::class)
    fun `ClientCallOptions can use scoped credentials`() {
        clientCallOptions {
            ByteArrayInputStream("{}".toByteArray()).use {
                withServiceAccountCredentials(it, listOf("scope"))
            }
        }
    }

    @Test
    fun `Can do a blocking call`() {
        val stub: TestStub = createTestStubMock()

        val call = GrpcClientStub(stub, ClientCallOptions())
        val result = call.executeBlocking { arg ->
            assertThat(arg).isEqualTo(stub)
            StringValue("hey there")
        }

        assertThat(result.body.value).isEqualTo("hey there")
    }

    @Test(expected = RuntimeException::class)
    fun `Throws on a failed a blocking call`() {
        val stub: TestStub = createTestStubMock()

        val call = GrpcClientStub(stub, ClientCallOptions())
        call.executeBlocking { arg ->
            assertThat(arg).isEqualTo(stub)
            throw RuntimeException("no good!")
        }
    }

    @Test
    fun `Can retry a blocking call`() {
        val stub: TestStub = createTestStubMock()
        val exception = RuntimeException("oh no!")

        val retry = object : Retry {
            var executed = false

            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                assertThat(error).isEqualTo(exception)
                assertThat(context.numberOfAttempts).isEqualTo(0)
                executed = true
                return 400
            }
        }

        val call = GrpcClientStub(stub, ClientCallOptions()).prepare {
            withRetry(retry)
        }
        val result: CallResult<StringValue> = call.executeBlocking { arg ->
            assertThat(arg).isEqualTo(stub)
            if (!retry.executed) {
                throw exception
            }
            StringValue("hey there again")
        }

        assertThat(result.body.value).isEqualTo("hey there again")
        assertThat(retry.executed).isTrue()
    }

    @Test
    fun `Can do a blocking call with metadata`() {
        val stub: TestStub = createTestStubMock()
        val options = clientCallOptions {
            withMetadata("one", listOf("two", "three"))
        }

        val call = GrpcClientStub(stub, options)
        val result = call.executeBlocking { arg ->
            verify(arg, times(2)).withInterceptors(any())
            StringValue("hi")
        }
        assertThat(result.body.value).isEqualTo("hi")
    }

    @Test
    fun `Can do a future call`() {
        val stub: TestStub = createTestStubMock()
        val future = SettableFuture.create<StringValue>()
        future.set(StringValue("hi"))

        val credentials: CallCredentials = mock()
        val interceptor: ClientInterceptor = mock()

        val call = GrpcClientStub(
            stub, ClientCallOptions(
                credentials = credentials,
                interceptors = listOf(interceptor),
                initialRequests = listOf("junk")
            )
        )
        val result = call.executeFuture { arg ->
            assertThat(arg).isEqualTo(stub)
            future
        }
        assertThat(result.get().body.value).isEqualTo("hi")
    }

    @Test(expected = IllegalStateException::class)
    fun `Throws on a failed future call`() {
        val stub: TestStub = createTestStubMock()
        val future = SettableFuture.create<StringValue>()
        future.setException(IllegalStateException("bad future"))

        val credentials: CallCredentials = mock()
        val interceptor: ClientInterceptor = mock()

        val call = GrpcClientStub(
            stub, ClientCallOptions(
                credentials = credentials,
                interceptors = listOf(interceptor),
                initialRequests = listOf("junk")
            )
        )
        val result = call.executeFuture { arg ->
            assertThat(arg).isEqualTo(stub)
            future
        }

        try {
            result.get()
        } catch (ex: Exception) {
            assertThat(ex).isInstanceOf(ExecutionException::class.java)
            throw ex.cause!!
        }
    }

    @Test
    fun `Can retry a future call`() {
        val stub: TestStub = createTestStubMock()
        val exception = IllegalArgumentException("bad news!")

        val future1 = SettableFuture.create<StringValue>()
        val future2 = SettableFuture.create<StringValue>()
        future1.setException(exception)
        future2.set(StringValue("hi again"))

        val credentials: CallCredentials = mock()
        val interceptor: ClientInterceptor = mock()

        val retry = object : Retry {
            var executed = false

            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                assertThat(error).isEqualTo(exception)
                assertThat(context.numberOfAttempts).isEqualTo(0)
                executed = true
                return 400
            }
        }

        val call = GrpcClientStub(
            stub, ClientCallOptions(
                credentials = credentials,
                interceptors = listOf(interceptor),
                initialRequests = listOf("junk"),
                retry = retry
            )
        )
        val result = call.executeFuture { arg ->
            assertThat(arg).isEqualTo(stub)
            if (!retry.executed) {
                future1
            } else {
                future2
            }
        }

        assertThat(result.get().body.value).isEqualTo("hi again")
        assertThat(retry.executed).isTrue()
    }

    @Test(expected = ExecutionException::class)
    fun `Throws on an invalid null future call`() {
        val stub: TestStub = createTestStubMock()
        val future = SettableFuture.create<StringValue>()
        future.set(null)

        val call = GrpcClientStub(stub, ClientCallOptions())
        call.executeFuture { _ -> future }.get()
    }

    @Test
    fun `Can do a long running call`() {
        val stub: TestStub = createTestStubMock()
        val operation = Operation.newBuilder()
            .setName("the op")
            .setDone(true)
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

    @Test
    fun `Can do a streaming call`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()
        val exception: RuntimeException = mock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        var outStream: StreamObserver<StringValue>? = null
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            outStream = outs
            return inStream
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        val responses = mutableListOf<String>()
        val exceptions = mutableListOf<Throwable>()
        var complete = false

        result.start {
            onNext = { responses.add(it.value) }
            onError = { exceptions.add(it) }
            onCompleted = { complete = true }
        }

        result.requests.send(Int32Value(1))
        result.requests.send(Int32Value(2))

        // fake output from server
        outStream?.onNext(StringValue("one"))
        outStream?.onNext(StringValue("two"))
        outStream?.onError(exception)
        outStream?.onCompleted()

        verify(inStream).onNext(Int32Value(1))
        verify(inStream).onNext(Int32Value(2))
        verify(inStream, never()).onCompleted()
        assertThat(responses).containsExactly("one", "two")
        assertThat(exceptions).containsExactly(exception)
        assertThat(complete).isTrue()

        // repeat with executor
        val executor: Executor = mock()
        result.responses.executor = executor
        outStream?.onNext(StringValue("4"))
        outStream?.onError(exception)
        outStream?.onCompleted()

        verify(executor, times(3)).execute(any())
        verify(clientCall, never()).cancel(any(), any())

        // close the steam
        result.responses.close()

        verify(clientCall).cancel(any(), check {
            assertThat(it).isInstanceOf(StreamingMethodClosedException::class.java)
        })
    }

    @Test
    fun `Can retry a streaming call`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()
        val exception: RuntimeException = mock()
        var timesExecuted = 0

        val retry = object : Retry {
            var executed = false

            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                assertThat(error).isEqualTo(exception)
                assertThat(context.numberOfAttempts).isEqualTo(timesExecuted - 1)
                executed = true
                return 100
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))

        var outStream: StreamObserver<StringValue>? = null
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            outStream = outs
            timesExecuted++
            return inStream
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        val responses = mutableListOf<String>()
        val exceptions = mutableListOf<Throwable>()
        var complete = false

        result.start {
            onNext = { responses.add(it.value) }
            onError = { exceptions.add(it) }
            onCompleted = { complete = true }
        }

        // fake output from server
        outStream?.onError(exception)
        Thread.sleep(200)
        outStream?.onError(exception)
        Thread.sleep(200)

        outStream?.onNext(StringValue("one"))
        outStream?.onNext(StringValue("two"))
        outStream?.onCompleted()

        verify(inStream, never()).onCompleted()
        assertThat(responses).containsExactly("one", "two")
        assertThat(exceptions).isEmpty()
        assertThat(complete).isTrue()

        assertThat(timesExecuted).isEqualTo(3)
        assertThat(retry.executed).isTrue()
    }

    @Test
    fun `Does not retry a streaming call that had a result`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()
        val exception1: RuntimeException = mock()
        val exception2: RuntimeException = mock()

        val retry = object : Retry {
            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                fail("retry not expected")
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))

        var timesExecuted = 0
        var outStream: StreamObserver<StringValue>? = null
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            outStream = outs
            timesExecuted++
            return inStream
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        val responses = mutableListOf<String>()
        val exceptions = mutableListOf<Throwable>()
        var complete = false

        result.start {
            onNext = { responses.add(it.value) }
            onError = { exceptions.add(it) }
            onCompleted = { complete = true }
        }

        // fake output from server
        outStream?.onNext(StringValue("result"))
        outStream?.onError(exception1)
        outStream?.onError(exception2)
        outStream?.onCompleted()

        verify(inStream, never()).onCompleted()
        assertThat(responses).containsExactly("result")
        assertThat(exceptions).containsExactly(exception1, exception2)
        assertThat(complete).isTrue()

        assertThat(timesExecuted).isEqualTo(1)
    }

    @Test
    fun `Can ignore streaming call events`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        var outStream: StreamObserver<StringValue>? = null
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            outStream = outs
            return inStream
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.start {
            onNext = { fail("onNext called") }
            onError = { fail("onError called") }
            onCompleted = { fail("onCompleted called") }
            ignoreIf = { true }
        }

        // fake output from server
        outStream?.onNext(StringValue("data"))
        outStream?.onError(RuntimeException())
        outStream?.onCompleted()

        // repeat for the individual flags
        for (prop in listOf("complete", "error", "next")) {
            var complete = false
            var error = false
            var next = false

            result.responses.let { stream ->
                stream.ignoreIf = { false }
                stream.ignoreNextIf = { _ -> prop == "next" }
                stream.ignoreErrorIf = { _ -> prop == "error" }
                stream.ignoreCompletedIf = { prop == "complete" }
                stream.onNext = { next = true }
                stream.onError = { error = true }
                stream.onCompleted = { complete = true }
            }

            outStream?.onNext(StringValue("stuff"))
            outStream?.onError(RuntimeException())
            outStream?.onCompleted()

            assertThat(next).isEqualTo(prop != "next")
            assertThat(error).isEqualTo(prop != "error")
            assertThat(complete).isEqualTo(prop != "complete")
        }
    }

    @Test
    fun `Can close a streaming call request stream`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        val method = { _: StreamObserver<StringValue> -> inStream }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            method
        }
        result.start()

        result.requests.send(Int32Value(5))
        result.requests.send(Int32Value(55))
        result.requests.close()

        verify(inStream).onNext(Int32Value(5))
        verify(inStream).onNext(Int32Value(55))
        verify(inStream).onCompleted()
    }

    @Test
    fun `Can send initial requests to a streaming call`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(
            stub, ClientCallOptions(
                initialRequests = listOf(Int32Value(9), Int32Value(99))
            )
        )
        val method = { _: StreamObserver<StringValue> -> inStream }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            method
        }
        result.start()

        result.requests.send(Int32Value(0))
        result.requests.close()

        verify(inStream).onNext(Int32Value(9))
        verify(inStream).onNext(Int32Value(99))
        verify(inStream).onNext(Int32Value(0))
        verify(inStream).onCompleted()
    }

    @Test(expected = kotlin.UninitializedPropertyAccessException::class)
    fun `Throws when a streaming call is not started`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        val method = { _: StreamObserver<StringValue> -> inStream }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            method
        }

        result.requests.send(Int32Value(1))
    }

    @Test
    fun `Can do a client streaming call`() {
        listOf(null, RuntimeException("failed")).forEach { ex ->
            val stub: TestStub = createTestStubMock()
            val inStream: StreamObserver<Int32Value> = mock()

            // capture output stream
            val call = GrpcClientStub(stub, ClientCallOptions())
            var outStream: StreamObserver<StringValue>? = null
            fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
                outStream = outs
                return inStream
            }

            val result = call.executeClientStreaming { arg ->
                assertThat(arg).isEqualTo(stub)
                ::method
            }

            result.start()
            result.requests.send(Int32Value(10))
            result.requests.send(Int32Value(20))

            // fake output from server
            if (ex != null) {
                outStream?.onError(ex)
            } else {
                outStream?.onNext(StringValue("abc"))
            }
            outStream?.onCompleted()

            verify(inStream).onNext(Int32Value(10))
            verify(inStream).onNext(Int32Value(20))
            if (ex != null) {
                assertFailsWith<ExecutionException>("failed") { result.response.get() }
            } else {
                assertThat(result.response.get().value).isEqualTo("abc")
            }
        }
    }

    @Test
    fun `Can retry a client streaming call`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()
        val exception = RuntimeException("it failed")
        var timesExecuted = 0

        val retry = object : Retry {
            var executed = false

            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                assertThat(error).isEqualTo(exception)
                assertThat(context.numberOfAttempts).isEqualTo(timesExecuted - 1)
                executed = true
                return 100
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))
        var outStream: StreamObserver<StringValue>? = null
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            outStream = outs
            timesExecuted++
            return inStream
        }

        val result = call.executeClientStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.start()
        result.requests.send(Int32Value(1))
        result.requests.send(Int32Value(2))

        // fake output from server
        outStream?.onError(exception)
        Thread.sleep(200)
        outStream?.onNext(StringValue("xyz"))
        outStream?.onCompleted()

        verify(inStream).onNext(Int32Value(1))
        verify(inStream).onNext(Int32Value(2))
        assertThat(result.response.get().value).isEqualTo("xyz")

        assertThat(retry.executed).isTrue()
        assertThat(timesExecuted).isEqualTo(2)
    }

    @Test
    fun `Does not retry a client streaming call after result`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        val retry = object : Retry {
            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                fail("retry not expected")
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))
        var outStream: StreamObserver<StringValue>? = null
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            outStream = outs
            return inStream
        }

        val result = call.executeClientStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.start()
        result.requests.send(Int32Value(1))
        result.requests.send(Int32Value(2))

        // fake output from server
        outStream?.onNext(StringValue("xyz"))
        outStream?.onError(RuntimeException("it failed"))
        Thread.sleep(200)
        outStream?.onCompleted()

        verify(inStream).onNext(Int32Value(1))
        verify(inStream).onNext(Int32Value(2))
        assertThat(result.response.get().value).isEqualTo("xyz")
    }

    @Test
    fun `Can close a client call request stream`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        val method = { _: StreamObserver<StringValue> -> inStream }

        val result = call.executeClientStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            method
        }
        result.start()

        result.requests.send(Int32Value(5))
        result.requests.send(Int32Value(55))
        result.requests.close()

        verify(inStream).onNext(Int32Value(5))
        verify(inStream).onNext(Int32Value(55))
        verify(inStream).onCompleted()
    }

    @Test
    fun `Can send initial requests to a client streaming call`() {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(
            stub, ClientCallOptions(
                initialRequests = listOf(Int32Value(1), Int32Value(2))
            )
        )
        val method = { _: StreamObserver<StringValue> -> inStream }

        val result = call.executeClientStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            method
        }
        result.start()

        result.requests.send(Int32Value(100))
        result.requests.close()

        verify(inStream).onNext(Int32Value(1))
        verify(inStream).onNext(Int32Value(2))
        verify(inStream).onNext(Int32Value(100))
        verify(inStream).onCompleted()
    }

    @Test(expected = kotlin.UninitializedPropertyAccessException::class)
    fun `Throws when a client streaming call is not started`() {
        listOf(null, RuntimeException("failed")).forEach { _ ->
            val stub: TestStub = createTestStubMock()
            val inStream: StreamObserver<Int32Value> = mock()

            // capture output stream
            val call = GrpcClientStub(stub, ClientCallOptions())
            val method = { _: StreamObserver<StringValue> -> inStream }

            val result = call.executeClientStreaming { arg ->
                assertThat(arg).isEqualTo(stub)
                method
            }

            result.requests.send(Int32Value.newBuilder().setValue(10).build())
            result.requests.send(Int32Value.newBuilder().setValue(20).build())
        }
    }

    @Test
    fun `Can do a server streaming call`() {
        val stub: TestStub = createTestStubMock()
        val exception: RuntimeException = mock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        var outStream: StreamObserver<StringValue>? = null
        val result = call.executeServerStreaming { it, observer: StreamObserver<StringValue> ->
            assertThat(it).isEqualTo(stub)
            outStream = observer
        }

        val responses = mutableListOf<String>()
        val exceptions = mutableListOf<Throwable>()
        var complete = false

        result.start {
            onNext = { responses.add(it.value) }
            onError = { exceptions.add(it) }
            onCompleted = { complete = true }
        }

        // fake output from server
        outStream?.onNext(StringValue("one"))
        outStream?.onNext(StringValue("two"))
        outStream?.onError(exception)
        outStream?.onCompleted()

        assertThat(responses).containsExactly("one", "two")
        assertThat(exceptions).containsExactly(exception)
        assertThat(complete).isTrue()

        // repeat with executor
        val executor: Executor = mock()
        result.responses.executor = executor
        outStream?.onNext(StringValue("8"))
        outStream?.onError(exception)
        outStream?.onCompleted()

        verify(executor, times(3)).execute(any())
        verify(clientCall, never()).cancel(any(), any())

        // close the steam
        result.responses.close()

        verify(clientCall).cancel(any(), check {
            assertThat(it).isInstanceOf(StreamingMethodClosedException::class.java)
        })
    }

    @Test
    fun `Can retry a server streaming call`() {
        val stub: TestStub = createTestStubMock()
        val exception: RuntimeException = mock()
        var timesExecuted = 0

        val retry = object : Retry {
            var executed = false

            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                assertThat(context.numberOfAttempts).isEqualTo(timesExecuted - 1)
                executed = true
                return 100
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))
        var outStream: StreamObserver<StringValue>? = null
        val result = call.executeServerStreaming { it, observer: StreamObserver<StringValue> ->
            assertThat(it).isEqualTo(stub)
            timesExecuted++
            outStream = observer
        }

        val responses = mutableListOf<String>()
        val exceptions = mutableListOf<Throwable>()
        var complete = false

        result.start {
            onNext = { responses.add(it.value) }
            onError = { exceptions.add(it) }
            onCompleted = { complete = true }
        }

        outStream?.onError(RuntimeException())
        Thread.sleep(200)
        outStream?.onError(RuntimeException())
        Thread.sleep(200)

        // fake output from server
        outStream?.onNext(StringValue("one"))
        outStream?.onNext(StringValue("two"))
        outStream?.onError(exception)
        outStream?.onCompleted()

        assertThat(responses).containsExactly("one", "two")
        assertThat(exceptions).containsExactly(exception)
        assertThat(complete).isTrue()

        assertThat(timesExecuted).isEqualTo(3)
        assertThat(retry.executed).isTrue()
    }

    @Test
    fun `Does not retry a server streaming call after a result`() {
        val stub: TestStub = createTestStubMock()
        val exception: RuntimeException = mock()
        var timesExecuted = 0

        val retry = object : Retry {
            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                fail("retry not expected")
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))
        var outStream: StreamObserver<StringValue>? = null
        val result = call.executeServerStreaming { it, observer: StreamObserver<StringValue> ->
            assertThat(it).isEqualTo(stub)
            timesExecuted++
            outStream = observer
        }

        val responses = mutableListOf<String>()
        val exceptions = mutableListOf<Throwable>()
        var complete = false

        result.start {
            onNext = { responses.add(it.value) }
            onError = { exceptions.add(it) }
            onCompleted = { complete = true }
        }

        // fake output from server
        outStream?.onNext(StringValue("one"))
        outStream?.onNext(StringValue("two"))
        outStream?.onError(exception)
        outStream?.onCompleted()

        assertThat(responses).containsExactly("one", "two")
        assertThat(exceptions).containsExactly(exception)
        assertThat(complete).isTrue()

        assertThat(timesExecuted).isEqualTo(1)
    }

    @Test
    fun `Can ignore server streaming call events`() {
        val stub: TestStub = createTestStubMock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        var outStream: StreamObserver<StringValue>? = null
        val result = call.executeServerStreaming { it, observer: StreamObserver<StringValue> ->
            assertThat(it).isEqualTo(stub)
            outStream = observer
        }

        result.start {
            onNext = { fail("onNext called") }
            onError = { fail("onError called") }
            onCompleted = { fail("onCompleted called") }
            ignoreIf = { true }
        }

        // fake output from server
        outStream?.onNext(StringValue("data"))
        outStream?.onError(RuntimeException())
        outStream?.onCompleted()

        // repeat for the individual flags
        for (prop in listOf("complete", "error", "next")) {
            var complete = false
            var error = false
            var next = false

            result.responses.let { stream ->
                stream.ignoreIf = { false }
                stream.ignoreNextIf = { _ -> prop == "next" }
                stream.ignoreErrorIf = { _ -> prop == "error" }
                stream.ignoreCompletedIf = { prop == "complete" }
                stream.onNext = { next = true }
                stream.onError = { error = true }
                stream.onCompleted = { complete = true }
            }

            outStream?.onNext(StringValue("stuff"))
            outStream?.onError(RuntimeException())
            outStream?.onCompleted()

            assertThat(next).isEqualTo(prop != "next")
            assertThat(error).isEqualTo(prop != "error")
            assertThat(complete).isEqualTo(prop != "complete")
        }
    }

    @Test(expected = UninitializedPropertyAccessException::class)
    fun `Throws when a server streaming call is not started`() {
        val stub: TestStub = createTestStubMock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        val result = call.executeServerStreaming { it, observer: StreamObserver<StringValue> ->
            assertThat(it).isEqualTo(stub)
        }

        val responses = mutableListOf<String>()
        val exceptions = mutableListOf<Throwable>()
        var complete = false

        // forget to call start
        result.responses.onNext = { responses.add(it.value) }
        result.responses.onError = { exceptions.add(it) }
        result.responses.onCompleted = { complete = true }
    }

    @Test
    fun `can be prepared`() {
        val stub = createTestStubMock()
        val otherStub = stub.prepare {
            withMetadata("a", listOf("one", "two"))
            withMetadata("other", listOf())
        }

        assertThat(stub).isNotEqualTo(otherStub)
        assertThat(otherStub.options.requestMetadata)
            .containsExactlyEntriesIn(
                mapOf(
                    "a" to listOf("one", "two"),
                    "other" to listOf()
                )
            )
    }

    @Test
    fun `can be directly prepared`() {
        val stub: TestStub = createTestStubMock()

        val call = GrpcClientStub(stub, ClientCallOptions())
        val otherCall = call.prepare {
            withMetadata("one", listOf("1"))
        }

        assertThat(call).isNotEqualTo(otherCall)
        assertThat(otherCall.options.requestMetadata)
            .containsExactlyEntriesIn(mapOf("one" to listOf("1")))
    }

    @Test
    fun `can be prepared with options`() {
        val stub: TestStub = createTestStubMock()

        val call = GrpcClientStub(stub, ClientCallOptions())
        val otherCall = call.prepare {
            withInitialRequest(StringValue("init!"))
        }

        assertThat(call).isNotEqualTo(otherCall)
        assertThat(otherCall.options.initialRequests)
            .containsExactly(StringValue("init!"))
    }

    @Test
    fun `can handle successful callbacks`() {
        val call = SettableFuture.create<CallResult<StringValue>>()
        var successValue: String? = null
        var errorValue: Throwable? = null

        call.on(MoreExecutors.directExecutor()) {
            success = { successValue = it.body.value }
            error = { errorValue = it }
        }
        call.set(CallResult(StringValue("hey"), ResponseMetadata()))

        assertThat(successValue).isEqualTo("hey")
        assertThat(errorValue).isNull()
    }

    @Test
    fun `can handle error callbacks`() {
        val call = SettableFuture.create<CallResult<StringValue>>()
        var successValue: String? = null
        var errorValue: Throwable? = null

        call.on(MoreExecutors.directExecutor()) {
            success = { successValue = it.body.value }
            error = { errorValue = it }
        }
        call.setException(IllegalStateException())

        assertThat(successValue).isNull()
        assertThat(errorValue).isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `can skip successful callbacks`() {
        val call = SettableFuture.create<CallResult<StringValue>>()

        call.on(MoreExecutors.directExecutor()) {
            success = { fail("success was called") }
            error = { fail("error was called") }
            ignoreIf = { true }
        }
        call.set(CallResult(StringValue("hey"), ResponseMetadata()))
    }

    @Test
    fun `can skip successful callbacks only`() {
        val call = SettableFuture.create<CallResult<StringValue>>()

        call.on(MoreExecutors.directExecutor()) {
            success = { fail("success was called") }
            error = { fail("error was called") }
            ignoreResultIf = { true }
        }
        call.set(CallResult(StringValue("you"), ResponseMetadata()))
    }

    @Test
    fun `can skip successful callbacks without confusion`() {
        val call = SettableFuture.create<CallResult<StringValue>>()
        var value: String? = null

        call.on(MoreExecutors.directExecutor()) {
            success = { value = it.body.value }
            error = { fail("error was called") }
            ignoreErrorIf = { true }
        }
        call.set(CallResult(StringValue("you"), ResponseMetadata()))

        assertThat(value).isEqualTo("you")
    }

    @Test
    fun `can skip error callbacks`() {
        val call = SettableFuture.create<CallResult<StringValue>>()

        call.on(MoreExecutors.directExecutor()) {
            success = { fail("success was called") }
            error = { fail("error was called") }
            ignoreIf = { true }
        }
        call.setException(RuntimeException())
    }

    @Test
    fun `can skip error callbacks only`() {
        val call = SettableFuture.create<CallResult<StringValue>>()

        call.on(MoreExecutors.directExecutor()) {
            success = { fail("success was called") }
            error = { fail("error was called") }
            ignoreErrorIf = { true }
        }
        call.setException(RuntimeException())
    }

    @Test
    fun `can skip error callbacks without confusion`() {
        val call = SettableFuture.create<CallResult<StringValue>>()
        var err: Throwable? = null

        call.on(MoreExecutors.directExecutor()) {
            success = { fail("success was called") }
            error = { err = it }
            ignoreResultIf = { true }
        }
        call.setException(RuntimeException())

        assertThat(err).isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `can get the result of self`() {
        val future: ListenableFuture<String> = mock {
            on { get() } doReturn "ok then"
        }
        future.get { assertThat(it).isEqualTo("ok then") }
    }

    private fun createTestStubMock(): TestStub {
        val stub: TestStub = mock()
        whenever(stub.channel).thenReturn(channel)
        whenever(stub.withInterceptors(any())).thenReturn(stub)
        whenever(stub.withCallCredentials(any())).thenReturn(stub)
        whenever(stub.withOption(any(), any<Any>())).thenReturn(stub)
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
