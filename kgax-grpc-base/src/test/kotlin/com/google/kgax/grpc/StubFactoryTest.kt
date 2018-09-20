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
import com.google.longrunning.OperationsGrpc
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import io.grpc.CallCredentials
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.ManagedChannelProvider
import io.grpc.stub.AbstractStub
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertFailsWith

class StubFactoryTest {

    @Test(expected = ManagedChannelProvider.ProviderNotFoundException::class)
    fun `creates stubs from factory`() {
        StubFactory(OperationsGrpc.OperationsFutureStub::class, "localhost")
    }

    @Test
    fun `Creates blocking stub from call credentials`() {
        val channel: ManagedChannel = mock()
        val factory = StubFactory(OperationsGrpc.OperationsFutureStub::class, channel)

        val credentials: CallCredentials = mock()
        val stub = factory.fromCallCredentials(credentials)

        assertThat(stub).isNotNull()
        assertThat(stub.options.credentials).isEqualTo(credentials)
    }

    @Test
    fun `Creates future stub from call credentials`() {
        val channel: ManagedChannel = mock()
        val factory = StubFactory(OperationsGrpc.OperationsFutureStub::class, channel)

        val credentials: CallCredentials = mock()
        val stub = factory.fromCallCredentials(credentials)

        assertThat(stub).isNotNull()
        assertThat(stub.options.credentials).isEqualTo(credentials)
    }

    @Test
    fun `Creates streaming stub from call credentials`() {
        val channel: ManagedChannel = mock()
        val factory = StubFactory(OperationsGrpc.OperationsStub::class, channel)

        val credentials: CallCredentials = mock()
        val stub = factory.fromCallCredentials(credentials)

        assertThat(stub).isNotNull()
        assertThat(stub.options.credentials).isEqualTo(credentials)
    }

    @Test
    fun `Throws on non stub type`() {
        val channel: ManagedChannel = mock()
        val factory = StubFactory(TestStub::class, channel)

        assertFailsWith(IllegalArgumentException::class) {
            factory.fromCallCredentials(mock())
        }
    }

    @Test
    fun `Shuts down the channel`() {
        val channel: ManagedChannel = mock()
        whenever(channel.shutdown()).thenReturn(channel)
        val channelBuilder: ManagedChannelBuilder<*> = mock {
            on { build() }.then { channel }
        }
        val factory = StubFactory(TestStub::class, channelBuilder)

        factory.shutdown(2)

        verify(channel).shutdown()
        verify(channel).awaitTermination(2, TimeUnit.SECONDS)
    }

    private class TestStub(channel: Channel, options: CallOptions) :
        AbstractStub<TestStub>(channel, options) {

        override fun build(channel: Channel, options: CallOptions): TestStub {
            return TestStub(channel, options)
        }
    }
}
