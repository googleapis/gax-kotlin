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

import com.google.api.kgax.grpc.StubFactory
import com.google.common.truth.Truth.assertThat
import com.google.showcase.v1alpha3.Blurb
import com.google.showcase.v1alpha3.ConnectRequest
import com.google.showcase.v1alpha3.CreateBlurbRequest
import com.google.showcase.v1alpha3.CreateRoomRequest
import com.google.showcase.v1alpha3.DeleteRoomRequest
import com.google.showcase.v1alpha3.MessagingGrpc
import com.google.showcase.v1alpha3.Room
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.AfterClass
import java.util.concurrent.TimeUnit
import kotlin.test.Test

/**
 * Integration tests via Showcase:
 * https://github.com/googleapis/gapic-showcase
 */
@ExperimentalCoroutinesApi
class MessagingTest {

    // client / connection to server
    companion object {
        private val host = System.getenv("HOST") ?: "localhost"
        private val port = System.getenv("PORT") ?: "7469"

        // use insecure client
        val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port.toInt())
            .usePlaintext()
            .build()

        val futureStub = StubFactory(
            MessagingGrpc.MessagingFutureStub::class, channel = channel
        ).newStub()

        val streamingStub = StubFactory(
            MessagingGrpc.MessagingStub::class, channel = channel
        ).newStub()

        @AfterClass
        @JvmStatic
        fun destroyClient() {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
        }
    }

    @Test
    fun `can have a quick chat`() = runBlocking<Unit> {
        // create a new room
        val room = futureStub.execute {
            it.createRoom(with(CreateRoomRequest.newBuilder()) {
                room = with(Room.newBuilder()) {
                    displayName = "room-${System.currentTimeMillis()}"
                    description = "for chatty folks"
                    build()
                }
                build()
            })
        }

        // connect to the room
        val streams = streamingStub.executeStreaming { it::connect }
        streams.requests.send(with(ConnectRequest.newBuilder()) {
            config = with(ConnectRequest.ConnectConfig.newBuilder()) {
                parent = room.name
                build()
            }
            build()
        })

        // aggregate all messages in the room
        val responses = mutableListOf<String>()
        val watchJob = launch {
            for (response in streams.responses) {
                val blurb = response.blurb
                responses.add("${blurb.user}: ${blurb.text}")
            }
        }

        listOf(
            "me" to "hi there!",
            "you" to "well, hello!",
            "me" to "alright, that's enough!",
            "you" to "bye!"
        ).forEach { (userName, message) ->
            futureStub.execute {
                it.createBlurb(with(CreateBlurbRequest.newBuilder()) {
                    parent = room.name
                    blurb = with(Blurb.newBuilder()) {
                        user = userName
                        text = message
                        build()
                    }
                    build()
                })
            }
        }

        // stop
        streams.requests.close()
        watchJob.join()
        futureStub.execute {
            it.deleteRoom(with(DeleteRoomRequest.newBuilder()) {
                name = room.name
                build()
            })
        }

        // verify
        assertThat(responses).containsExactly(
            "me: hi there!",
            "you: well, hello!",
            "me: alright, that's enough!",
            "you: bye!"
        ).inOrder()
    }
}
