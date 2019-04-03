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

import com.google.api.kgax.Page
import com.google.api.kgax.Retry
import com.google.api.kgax.RetryContext
import com.google.common.truth.Truth.assertThat
import com.google.common.util.concurrent.SettableFuture
import com.google.protobuf.Int32Value
import com.google.protobuf.StringValue
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.check
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.never
import com.nhaarman.mockito_kotlin.reset
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import io.grpc.CallCredentials
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.Metadata
import io.grpc.stub.AbstractStub
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.map
import kotlinx.coroutines.channels.toList
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.io.ByteArrayInputStream
import java.io.IOException
import java.util.concurrent.CancellationException
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.fail

private fun string(value: String): StringValue = StringValue.newBuilder().setValue(value).build()
private fun int32(value: Int): Int32Value = Int32Value.newBuilder().setValue(value).build()

private data class PagedRequestType(val query: String, val token: String? = null)
private typealias PagedResponseType = Page<Int>

@ExperimentalCoroutinesApi
class GrpcClientStubTest {

    private val channel: Channel = mock()
    private val clientCall: ClientCall<*, *> = mock()
    private val callOptions: CallOptions = mock()
    private val callContext: ClientCallContext = mock()

    @BeforeTest
    fun before() {
        reset(channel, clientCall, callOptions, callContext)
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
    fun `ClientCallOptions can capture headers`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val future = SettableFuture.create<Int32Value>()
        future.set(int32(12345))

        var gotHeaders = false
        val call = GrpcClientStub(
            stub, clientCallOptions {
                onResponseMetadata { metadata: ResponseMetadata ->
                    gotHeaders = true
                    assertThat(metadata.keys()).containsExactly("yek")
                    assertThat(metadata.get("yek")).isEqualTo("a key")
                    assertThat(metadata.getAll("yek")).containsExactly("a key")
                    assertThat(metadata.get("key")).isNull()
                    assertThat(metadata.getAll("key")).isNull()
                }
            }
        )

        val result = call.execute { arg ->
            verify(arg).withOption(eq(ClientCallContext.KEY), check { ctx ->
                with(Metadata()) {
                    put(Metadata.Key.of("yek", Metadata.ASCII_STRING_MARSHALLER), "a key")
                    ctx.onResponseHeaders(this)
                }
            })
            future
        }
        assertThat(gotHeaders).isTrue()
        assertThat(result.value).isEqualTo(12345)
    }

    @Test
    fun `ClientCallOptions can capture multiple headers`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val future = SettableFuture.create<Int32Value>()
        future.set(int32(12345))

        var gotHeaders = false
        val call = GrpcClientStub(
            stub, clientCallOptions {
                onResponseMetadata { metadata: ResponseMetadata ->
                    gotHeaders = true
                    assertThat(metadata.keys()).containsExactly("one", "two", "three")
                    assertThat(metadata.get("one")).isEqualTo("111")
                    assertThat(metadata.getAll("one")).containsExactly("1", "11", "111").inOrder()
                    assertThat(metadata.get("two")).isEqualTo("22")
                    assertThat(metadata.getAll("two")).containsExactly("2", "22").inOrder()
                    assertThat(metadata.get("three")).isEqualTo("3")
                    assertThat(metadata.getAll("three")).containsExactly("3").inOrder()
                }
            }
        )

        val result = call.execute { arg ->
            verify(arg).withOption(eq(ClientCallContext.KEY), check { ctx ->
                with(Metadata()) {
                    put(Metadata.Key.of("one", Metadata.ASCII_STRING_MARSHALLER), "1")
                    put(Metadata.Key.of("one", Metadata.ASCII_STRING_MARSHALLER), "11")
                    put(Metadata.Key.of("one", Metadata.ASCII_STRING_MARSHALLER), "111")
                    put(Metadata.Key.of("two", Metadata.ASCII_STRING_MARSHALLER), "2")
                    put(Metadata.Key.of("two", Metadata.ASCII_STRING_MARSHALLER), "22")
                    put(Metadata.Key.of("three", Metadata.ASCII_STRING_MARSHALLER), "3")
                    ctx.onResponseHeaders(this)
                }
            })
            future
        }
        assertThat(gotHeaders).isTrue()
        assertThat(result.value).isEqualTo(12345)
    }

    @Test
    fun `ClientCallOptions can capture customized headers`() = runBlocking<Unit> {
        class MyMetadata(metadata: Metadata) : ResponseMetadata(metadata) {
            val yek: String
                get() = get("yek")!!
            val all: List<String>
                get() = getAll("yek")!!.toList()
        }

        val stub: TestStub = createTestStubMock()
        val future = SettableFuture.create<Int32Value>()
        future.set(int32(12345))

        var gotHeaders = false
        val call = GrpcClientStub(
            stub, clientCallOptions {
                onResponseMetadata(factory = {
                    m -> MyMetadata(m)
                }) { metadata: MyMetadata ->
                    gotHeaders = true
                    assertThat(metadata.keys()).containsExactly("yek")
                    assertThat(metadata.get("yek")).isEqualTo("a key")
                    assertThat(metadata.yek).isEqualTo("a key")
                    assertThat(metadata.getAll("yek")).containsExactly("a key")
                    assertThat(metadata.all).containsExactly("a key")
                    assertThat(metadata.get("key")).isNull()
                    assertThat(metadata.getAll("key")).isNull()
                }
            }
        )

        val result = call.execute { arg ->
            verify(arg).withOption(eq(ClientCallContext.KEY), check { ctx ->
                with(Metadata()) {
                    put(Metadata.Key.of("yek", Metadata.ASCII_STRING_MARSHALLER), "a key")
                    ctx.onResponseHeaders(this)
                }
            })
            future
        }
        assertThat(gotHeaders).isTrue()
        assertThat(result.value).isEqualTo(12345)
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
            withInitialRequest(string("!"))
            withMetadata("a", listOf("aa", "aaa"))
            withMetadata("b", listOf("bb"))
            withRetry(retry)
        }

        val newOpts = ClientCallOptions.Builder(opts).build()

        assertThat(opts).isNotEqualTo(newOpts)
        assertThat(newOpts.interceptors).containsExactly(interceptor1, interceptor2)
        assertThat(newOpts.initialRequests).containsExactly(string("!"))
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
    fun `Can do a deferred call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val future = SettableFuture.create<StringValue>()
        future.set(string("hi"))

        val credentials: CallCredentials = mock()
        val interceptor: ClientInterceptor = mock()

        val call = GrpcClientStub(
            stub, ClientCallOptions(
                credentials = credentials,
                interceptors = listOf(interceptor),
                initialRequests = listOf("junk")
            )
        )
        val result = call.execute { arg ->
            assertThat(arg).isEqualTo(stub)
            future
        }
        assertThat(result.value).isEqualTo("hi")
    }

    @Test(expected = IllegalStateException::class)
    fun `Throws on a failed deferred call`() = runBlocking<Unit> {
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

        try {
            call.execute { arg ->
                assertThat(arg).isEqualTo(stub)
                future
            }
        } catch (ex: Exception) {
            assertThat(ex).isInstanceOf(IllegalStateException::class.java)
            throw ex
        }
    }

    @Test
    fun `Can retry a deferred call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val exception = IllegalArgumentException("bad news!")

        val future1 = SettableFuture.create<StringValue>()
        val future2 = SettableFuture.create<StringValue>()
        future1.setException(exception)
        future2.set(string("hi again"))

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
        val result = call.execute { arg ->
            assertThat(arg).isEqualTo(stub)
            if (!retry.executed) {
                future1
            } else {
                future2
            }
        }

        assertThat(result.value).isEqualTo("hi again")
        assertThat(retry.executed).isTrue()
    }

    @Test
    fun `Does not retry a cancelled deferred call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()

        val retry = object : Retry {
            override fun retryAfter(error: Throwable, context: RetryContext): Long? = fail("should not retry")
        }

        val call = GrpcClientStub(
            stub, ClientCallOptions(
                credentials = mock(),
                retry = retry
            )
        )

        val job = Job()
        launch(job) {
            call.execute { arg ->
                job.cancel()
                SettableFuture.create<StringValue>()
            }
        }.join()
        assertThat(job.isCancelled).isTrue()
    }

    @Test
    fun `Can do a streaming call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            // fake output from server
            outs.onNext(string("one"))
            afterDelay {
                outs.onNext(string("two"))
                outs.onCompleted()
            }

            return inStream
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.requests.send(int32(1))
        result.requests.send(int32(2))

        result.join()

        verify(inStream).onNext(int32(1))
        verify(inStream).onNext(int32(2))
        verify(inStream).onCompleted()
        verify(clientCall, never()).cancel(any(), any())
        assertThat(result.responses.map { it.value }.toList()).containsExactly("one", "two").inOrder()
        assertThat(result.responses.isClosedForReceive).isTrue()
        assertThat(result.requests.isClosedForSend).isTrue()
    }

    @Test
    fun `Can close a streaming call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        fun method(observer: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            return mock()
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        // close the steam
        result.responses.cancel()

        result.join()

        verify(clientCall).cancel(any(), check {
            assertThat(it).isInstanceOf(CancellationException::class.java)
        })
    }

    @Test
    fun `Can retry a streaming call`() = runBlocking<Unit> {
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
                return 50
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))

        // var outStream: StreamObserver<StringValue>? = null
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            timesExecuted++

            // fake output from server
            if (timesExecuted < 2) {
                afterDelay { outs.onError(exception) }
            } else if (timesExecuted < 3) {
                outs.onError(exception)
            } else {
                outs.onNext(string("one"))
                outs.onNext(string("two"))
                afterDelay { outs.onCompleted() }
            }

            return inStream
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.join()

        verify(inStream).onCompleted()
        assertThat(result.responses.map { it.value }.toList()).containsExactly("one", "two").inOrder()
        assertThat(result.responses.isClosedForReceive).isTrue()
        assertThat(result.requests.isClosedForSend).isTrue()

        assertThat(timesExecuted).isEqualTo(3)
        assertThat(retry.executed).isTrue()
    }

    @Test
    fun `Does not retry a streaming call that had a result`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()
        val exception1 = RuntimeException("bad times")

        val retry = object : Retry {
            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                fail("retry not expected")
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))

        var timesExecuted = 0
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            timesExecuted++

            // fake output from server
            afterDelay {
                outs.onNext(string("result"))
                outs.onError(exception1)
                outs.onCompleted()
            }

            return inStream
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.join()

        verify(inStream).onCompleted()
        assertFailsWith<RuntimeException>("bad times") {
            runBlocking { result.responses.toList() }
        }
        assertThat(result.responses.isClosedForReceive).isTrue()
        assertThat(result.requests.isClosedForSend).isTrue()

        assertThat(timesExecuted).isEqualTo(1)
    }

    @Test
    fun `Can close a streaming call request stream`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            // fake output from server
            afterDelay {
                outs.onCompleted()
            }

            return inStream
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.requests.send(int32(5))
        result.requests.send(int32(55))
        result.requests.close()

        result.join()

        verify(inStream).onNext(int32(5))
        verify(inStream).onNext(int32(55))
        verify(inStream).onCompleted()
    }

    @Test
    fun `Can send initial requests to a streaming call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(
            stub, ClientCallOptions(
                initialRequests = listOf(int32(9), int32(99))
            )
        )

        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            // fake output from server
            afterDelay {
                outs.onCompleted()
            }

            return inStream
        }

        val result = call.executeStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.requests.send(int32(0))
        result.requests.close()

        result.join()

        verify(inStream).onNext(int32(9))
        verify(inStream).onNext(int32(99))
        verify(inStream).onNext(int32(0))
        verify(inStream).onCompleted()
    }

    @Test
    fun `Can do a client streaming call`() = runBlocking<Unit> {
        listOf(null, IllegalArgumentException("failed")).forEach { ex ->
            val stub: TestStub = createTestStubMock()
            val inStream: StreamObserver<Int32Value> = mock()

            // capture output stream
            val call = GrpcClientStub(stub, ClientCallOptions())
            fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
                // fake output from server
                afterDelay {
                    if (ex != null) {
                        outs.onError(ex)
                    } else {
                        outs.onNext(string("abc"))
                    }
                    outs.onCompleted()
                }

                return inStream
            }

            val result = call.executeClientStreaming { arg ->
                assertThat(arg).isEqualTo(stub)
                ::method
            }

            result.requests.send(int32(10))
            result.requests.send(int32(20))

            result.join()

            verify(inStream).onNext(int32(10))
            verify(inStream).onNext(int32(20))
            if (ex != null) {
                assertFailsWith<IllegalArgumentException>("failed") { runBlocking { result.response.await() } }
            } else {
                assertThat(result.response.await().value).isEqualTo("abc")
            }
        }
    }

    @Test
    fun `Can retry a client streaming call`() = runBlocking<Unit> {
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
                return 50
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))

        val canSend = waiter()
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            timesExecuted++

            // fake output from server
            if (timesExecuted < 2) {
                outs.onError(exception)
            } else {
                canSend.done()
                afterDelay {
                    outs.onNext(string("xyz"))
                    outs.onCompleted()
                }
            }

            return inStream
        }

        val result = call.executeClientStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        canSend.await()

        result.requests.send(int32(1))
        result.requests.send(int32(2))

        result.join()

        verify(inStream).onNext(int32(1))
        verify(inStream).onNext(int32(2))
        assertThat(result.response.await().value).isEqualTo("xyz")

        assertThat(retry.executed).isTrue()
        assertThat(timesExecuted).isEqualTo(2)
    }

    @Test
    fun `Does not retry a client streaming call after send`() = runBlocking<Unit> {
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
                return 50
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))

        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            timesExecuted++

            // fake output from server
            afterDelay {
                if (timesExecuted < 2) {
                    outs.onError(exception)
                } else {
                    outs.onNext(string("xyz"))
                    outs.onCompleted()
                }
            }

            return inStream
        }

        val result = call.executeClientStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.requests.send(int32(100))
        result.requests.send(int32(200))

        result.join()

        verify(inStream).onNext(int32(100))
        verify(inStream).onNext(int32(200))

        assertThat(retry.executed).isFalse()
        assertThat(timesExecuted).isEqualTo(1)
    }

    @Test
    fun `Does not retry a client streaming call after result`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        val retry = object : Retry {
            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                fail("retry not expected")
            }
        }

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions(retry = retry))

        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            // fake output from server
            afterDelay {
                outs.onNext(string("xyz"))
                outs.onError(RuntimeException("it failed"))
                outs.onCompleted()
            }

            return inStream
        }

        val result = call.executeClientStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.requests.send(int32(1))
        result.requests.send(int32(2))

        result.join()

        verify(inStream).onNext(int32(1))
        verify(inStream).onNext(int32(2))
        assertThat(result.response.await().value).isEqualTo("xyz")
    }

    @Test
    fun `Can close a client call request stream`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            afterDelay {
                outs.onCompleted()
            }

            return inStream
        }

        val result = call.executeClientStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.requests.send(int32(5))
        result.requests.send(int32(55))
        result.requests.close()

        result.join()

        verify(inStream).onNext(int32(5))
        verify(inStream).onNext(int32(55))
        verify(inStream).onCompleted()
    }

    @Test
    fun `Can send initial requests to a client streaming call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        val inStream: StreamObserver<Int32Value> = mock()

        // capture output stream
        val call = GrpcClientStub(
            stub, ClientCallOptions(
                initialRequests = listOf(int32(1), int32(2))
            )
        )

        fun method(outs: StreamObserver<StringValue>): StreamObserver<Int32Value> {
            // fake output from the server
            outs.onNext(string("hi!"))
            afterDelay {
                outs.onCompleted()
            }

            return inStream
        }

        val result = call.executeClientStreaming { arg ->
            assertThat(arg).isEqualTo(stub)
            ::method
        }

        result.requests.send(int32(100))
        result.requests.close()

        result.join()

        verify(inStream).onNext(int32(1))
        verify(inStream).onNext(int32(2))
        verify(inStream).onNext(int32(100))
        verify(inStream).onCompleted()
    }

    @Test
    fun `Can do a server streaming call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()

        // capture output stream
        val call = GrpcClientStub(stub, ClientCallOptions())
        val result = call.executeServerStreaming { it, outs: StreamObserver<StringValue> ->
            assertThat(it).isEqualTo(stub)

            // fake output from server
            outs.onNext(string("one"))
            afterDelay {
                outs.onNext(string("two"))
                outs.onCompleted()
            }
        }

        result.join()

        assertThat(result.responses.map { it.value }.toList()).containsExactly("one", "two").inOrder()
        assertThat(result.responses.isClosedForReceive).isTrue()

        verify(clientCall, never()).cancel(any(), any())
    }

    @Test
    fun `Can close a server streaming call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()

        val call = GrpcClientStub(stub, ClientCallOptions())
        val result = call.executeServerStreaming { it, _: StreamObserver<StringValue> ->
            mock()
        }

        // close the steam
        result.responses.cancel()

        result.join()

        verify(clientCall).cancel(any(), check {
            assertThat(it).isInstanceOf(CancellationException::class.java)
        })
    }

    @Test
    fun `Can retry a server streaming call`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
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
        val result = call.executeServerStreaming { it, outs: StreamObserver<StringValue> ->
            assertThat(it).isEqualTo(stub)
            timesExecuted++

            // fake output from server
            afterDelay {
                if (timesExecuted < 3) {
                    outs.onError(RuntimeException())
                } else {
                    outs.onNext(string("one"))
                    outs.onNext(string("two"))
                    outs.onCompleted()
                }
            }
        }

        result.join()

        assertThat(result.responses.map { it.value }.toList()).containsExactly("one", "two").inOrder()
        assertThat(result.responses.isClosedForReceive).isTrue()

        assertThat(timesExecuted).isEqualTo(3)
        assertThat(retry.executed).isTrue()
    }

    @Test
    fun `Does not retry a server streaming call after a result`() = runBlocking<Unit> {
        val stub: TestStub = createTestStubMock()
        var timesExecuted = 0

        val retry = object : Retry {
            override fun retryAfter(error: Throwable, context: RetryContext): Long? {
                fail("retry not expected")
            }
        }

        // capture output stream
        val call =
            GrpcClientStub(stub, ClientCallOptions(retry = retry))
        val result = call.executeServerStreaming { it, outs: StreamObserver<StringValue> ->
            assertThat(it).isEqualTo(stub)
            timesExecuted++

            // fake output from server
            outs.onNext(string("one"))
            afterDelay {
                outs.onNext(string("two"))
                outs.onCompleted()
            }
        }

        result.join()

        assertThat(result.responses.map { it.value }.toList()).containsExactly("one", "two").inOrder()
        assertThat(result.responses.isClosedForReceive).isTrue()

        assertThat(timesExecuted).isEqualTo(1)
    }

    @Test
    fun `can be paged`() = runBlocking<Unit> {
        val request = PagedRequestType("1")

        suspend fun method(request: PagedRequestType) = withContext(Dispatchers.Default) {
            when (request.token) {
                null -> Page(listOf(1, 2), "first")
                "first" -> Page(listOf(3, 4), "second")
                else -> Page(listOf(5, 6), "")
            }
        }

        var count = 0
        val pager =
            pager(
                method = ::method,
                initialRequest = {
                    request
                },
                nextRequest = { r, token ->
                    assertThat(r).isEqualTo(request)
                    PagedRequestType(r.query, token)
                },
                nextPage = { response ->
                    count++
                    Page(response.elements, response.token!!)
                }
            )

        val results = mutableListOf<Int>()
        for (page in pager) {
            for (entry in page.elements) {
                results.add(entry)
            }
        }
        assertThat(results).containsExactly(1, 2, 3, 4, 5, 6).inOrder()
    }

    @Test
    fun `can get a stub with context`() {
        val stub: TestStub = mock()
        whenever(stub.withInterceptors(any())).doReturn(stub)
        whenever(stub.withOption<ClientCallContext>(any(), any())).doReturn(stub)
        whenever(stub.withCallCredentials(any())).doReturn(stub)

        val options = ClientCallOptions()
        val clientStub = GrpcClientStub(stub, options)

        val newStub = clientStub.stubWithContext()

        verify(stub).withInterceptors(check { assertThat(it).isInstanceOf(GAXInterceptor::class.java) })
        verify(stub).withOption<ClientCallContext>(eq(ClientCallContext.KEY), check {
            assertThat(it).isInstanceOf(ClientCallContext::class.java)
        })
        verify(stub, never()).withCallCredentials(any())

        assertThat(newStub).isEqualTo(stub)
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
    fun `can be prepared directly`() {
        val stub = createTestStubMock()
        val otherStub = stub.prepare(
            ClientCallOptions(
                requestMetadata = mapOf(
                    "a" to listOf("one", "two"),
                    "other" to listOf()
                )
            )
        )

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
            withInitialRequest(string("init!"))
        }

        assertThat(call).isNotEqualTo(otherCall)
        assertThat(otherCall.options.initialRequests)
            .containsExactly(string("init!"))
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
        return stub
    }

    private class TestStub(channel: Channel, options: CallOptions) :
        AbstractStub<TestStub>(channel, options) {

        override fun build(channel: Channel, options: CallOptions): TestStub {
            return TestStub(channel, options)
        }
    }
}

// helpers for waiting during tests
private fun waiter() = CompletableDeferred<Unit>()

private fun CompletableDeferred<Unit>.done() = this.complete(Unit)

private fun CoroutineScope.afterDelay(
    delayInMillis: Long = 200,
    block: suspend () -> Unit
) = launch {
    delay(delayInMillis)
    block()
}
