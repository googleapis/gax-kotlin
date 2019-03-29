/*
 * Copyright 2019 Google LLC
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

package com.google.api.kotlin

import com.google.api.kgax.grpc.ServerStreamingCall
import com.google.api.kgax.grpc.StubFactory
import com.google.common.truth.Truth.assertThat
import com.google.rpc.Code
import com.google.rpc.Status
import com.google.showcase.v1alpha3.EchoGrpc
import com.google.showcase.v1alpha3.EchoRequest
import com.google.showcase.v1alpha3.EchoResponse
import com.google.showcase.v1alpha3.ExpandRequest
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.AfterClass
import java.util.Random
import java.util.concurrent.TimeUnit
import kotlin.test.Test

/**
 * Integration tests via Showcase:
 * https://github.com/googleapis/gapic-showcase
 *
 * Note: gapic-generator-kotlin contains all these tests, and more.
 * Only a subset is maintained here.
 */
@ExperimentalCoroutinesApi
class EchoTest {

    // client / connection to server
    companion object {
        private val host = System.getenv("HOST") ?: "localhost"
        private val port = System.getenv("PORT") ?: "7469"

        // use insecure client
        val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port.toInt())
            .usePlaintext()
            .build()

        val futureStub = StubFactory(
            EchoGrpc.EchoFutureStub::class, channel = channel
        ).newStub()

        val streamingStub = StubFactory(
            EchoGrpc.EchoStub::class, channel = channel
        ).newStub()

        @AfterClass
        @JvmStatic
        fun destroyClient() {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
        }
    }

    @Test
    fun `echos a request`() = runBlocking<Unit> {
        val result = futureStub.execute {
            it.echo(with(EchoRequest.newBuilder()) {
                content = "Hi there!"
                build()
            })
        }

        assertThat(result.body.content).isEqualTo("Hi there!")
    }

    @Test(expected = StatusRuntimeException::class)
    fun `throws an error`() = runBlocking<Unit> {
        try {
            futureStub.execute {
                it.echo(
                    with(EchoRequest.newBuilder()) {
                        content = "junk"
                        error = with(Status.newBuilder()) {
                            code = Code.DATA_LOSS_VALUE
                            message = "oh no!"
                            build()
                        }
                        build()
                    })
            }
        } catch (error: StatusRuntimeException) {
            assertThat(error.message).isEqualTo("DATA_LOSS: oh no!")
            assertThat(error.status.code.value()).isEqualTo(Code.DATA_LOSS_VALUE)
            throw error
        }
    }

    @Test
    fun `can expand a stream of responses`() = runBlocking<Unit> {
        val expansions = mutableListOf<String>()

        val streams: ServerStreamingCall<EchoResponse> = streamingStub.executeServerStreaming { stub, observer ->
            stub.expand(with(ExpandRequest.newBuilder()) {
                content = "well hello there how are you"
                build()
            }, observer)
        }

        for (response in streams.responses) {
            expansions.add(response.content)
        }

        assertThat(expansions).containsExactly("well", "hello", "there", "how", "are", "you").inOrder()
    }

    @Test
    fun `can expand a stream of responses and then error`() = runBlocking<Unit> {
        val expansions = mutableListOf<String>()

        val streams: ServerStreamingCall<EchoResponse> = streamingStub.executeServerStreaming { stub, observer ->
            stub.expand(with(ExpandRequest.newBuilder()) {
                content = "one two zee"
                error = with(Status.newBuilder()) {
                    code = Code.ABORTED_VALUE
                    message = "yikes"
                    build()
                }
                build()
            }, observer)
        }

        var error: Throwable? = null
        try {
            for (response in streams.responses) {
                expansions.add(response.content)
            }
        } catch (t: Throwable) {
            error = t
        }

        assertThat(error).isNotNull()
        assertThat(expansions).containsExactly("one", "two", "zee").inOrder()
    }

    @Test
    fun `can collect a stream of requests`() = runBlocking<Unit> {
        val streams = streamingStub.executeClientStreaming { it::collect }

        listOf("a", "b", "c", "done").map {
            streams.requests.send(with(EchoRequest.newBuilder()) {
                content = it
                build()
            })
        }
        streams.requests.close()

        assertThat(streams.response.await().content).isEqualTo("a b c done")
    }

    @Test
    fun `can have a random chat`() = runBlocking<Unit> {
        val inputs = Array(5) { _ ->
            Random().ints(20)
                .boxed()
                .map { it.toString() }
                .toArray()
                .joinToString("->")
        }

        val streams = streamingStub.executeStreaming { it::chat }

        launch {
            for (str in inputs) {
                streams.requests.send(with(EchoRequest.newBuilder()) {
                    content = str
                    build()
                })
            }
            streams.requests.close()
        }

        val responses = mutableListOf<String>()
        for (response in streams.responses) {
            responses.add(response.content)
        }

        assertThat(responses).containsExactly(*inputs).inOrder()
    }
}
