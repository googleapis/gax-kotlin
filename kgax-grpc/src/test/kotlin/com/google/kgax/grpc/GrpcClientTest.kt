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

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import io.grpc.ManagedChannel
import java.util.concurrent.TimeUnit
import kotlin.test.Test

class GrpcClientTest {

    @Test
    fun `Shuts down the channel`() {
        val channel: ManagedChannel = mock {
            on { awaitTermination(any(), any()) }.thenReturn(true)
        }
        whenever(channel.shutdown()).thenReturn(channel)

        val client = TestClient(channel, mock())
        client.shutdownChannel(8)

        verify(channel).shutdown()
        verify(channel).awaitTermination(8, TimeUnit.SECONDS)
    }

    private class TestClient(channel: ManagedChannel,
                             options: ClientCallOptions
    ) : GrpcClient(channel, options)

}