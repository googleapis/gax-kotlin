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
import com.google.common.util.concurrent.SettableFuture
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.stub.AbstractStub
import io.grpc.stub.StreamObserver
import java.util.concurrent.ExecutionException
import kotlin.test.Test
import kotlin.test.assertFailsWith

class ClientCallTest {

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
    fun `Can do a blocking call`() {
        val stub: TestStub = mock()

        val call = ClientCall(stub, mock())
        val result = call.executeBlocking { arg ->
            assertThat(arg).isEqualTo(stub)
            "hey there"
        }
        assertThat(result.body).isEqualTo("hey there")
    }

    @Test
    fun `Can do a future call`() {
        val stub: TestStub = mock()
        val future = SettableFuture.create<String>()
        future.set("hi")

        val call = ClientCall(stub, mock())
        val result = call.executeFuture { arg ->
            assertThat(arg).isEqualTo(stub)
            future
        }
        assertThat(result.get().body).isEqualTo("hi")
    }

    @Test
    fun `Can do a streaming call`() {
        val stub: TestStub = mock()
        val inStream: StreamObserver<Int> = mock()
        val exception: RuntimeException = mock()

        // capture output stream
        val call = ClientCall(stub, mock())
        var outStream: StreamObserver<String>? = null
        fun method(outs: StreamObserver<String>): StreamObserver<Int> {
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
        result.responses.onNext = { responses.add(it) }
        result.responses.onError = { exceptions.add(it) }
        result.responses.onCompleted = { complete = true }
        result.requests.send(1)
        result.requests.send(2)

        // fake output from server
        outStream?.onNext("one")
        outStream?.onNext("two")
        outStream?.onError(exception)
        outStream?.onCompleted()

        verify(inStream).onNext(1)
        verify(inStream).onNext(2)
        assertThat(responses).containsExactly("one", "two")
        assertThat(exceptions).containsExactly(exception)
        assertThat(complete).isTrue()
    }

    @Test
    fun `Can do a client streaming call`() {
        listOf(null, RuntimeException("failed")).forEach { ex ->
            val stub: TestStub = mock()
            val inStream: StreamObserver<Int> = mock()

            // capture output stream
            val call = ClientCall(stub, mock())
            var outStream: StreamObserver<String>? = null
            fun method(outs: StreamObserver<String>): StreamObserver<Int> {
                outStream = outs
                return inStream
            }

            val result = call.executeClientStreaming { arg ->
                assertThat(arg).isEqualTo(stub)
                ::method
            }

            result.requests.send(10)
            result.requests.send(20)

            // fake output from server
            if (ex != null) {
                outStream?.onError(ex)
            } else {
                outStream?.onNext("abc")
            }
            outStream?.onCompleted()

            verify(inStream).onNext(10)
            verify(inStream).onNext(20)
            if (ex != null) {
                assertFailsWith<ExecutionException>("failed") { result.response.get() }
            } else {
                assertThat(result.response.get()).isEqualTo("abc")
            }
        }
    }

    @Test
    fun `Can do a server streaming call`() {
        val stub: TestStub = mock()
        val exception: RuntimeException = mock()

        // capture output stream
        val call = ClientCall(stub, mock())
        var outStream: StreamObserver<String>? = null
        val result = call.executeServerStreaming { it, observer: StreamObserver<String> ->
            assertThat(it).isEqualTo(stub)
            outStream = observer
        }

        val responses = mutableListOf<String>()
        val exceptions = mutableListOf<Throwable>()
        var complete = false
        result.responses.onNext = { responses.add(it) }
        result.responses.onError = { exceptions.add(it) }
        result.responses.onCompleted = { complete = true }

        // fake output from server
        outStream?.onNext("one")
        outStream?.onNext("two")
        outStream?.onError(exception)
        outStream?.onCompleted()

        assertThat(responses).containsExactly("one", "two")
        assertThat(exceptions).containsExactly(exception)
        assertThat(complete).isTrue()
    }

    class TestStub : AbstractStub<TestStub> {
        constructor(channel: Channel) : super(channel)
        constructor(channel: Channel, options: CallOptions) : super(channel, options)

        override fun build(channel: Channel, options: CallOptions): TestStub {
            return TestStub(channel, options)
        }
    }
}