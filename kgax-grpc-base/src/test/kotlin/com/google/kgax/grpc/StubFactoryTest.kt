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

import com.google.auth.oauth2.AccessToken
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
import java.io.ByteArrayInputStream
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertFailsWith

class StubFactoryTest {

    @Test(expected = ManagedChannelProvider.ProviderNotFoundException::class)
    fun `creates stubs from factory`() {
        StubFactory(OperationsGrpc.OperationsFutureStub::class, "localhost")
    }

    @Test(expected = ManagedChannelProvider.ProviderNotFoundException::class)
    fun `creates stubs from factory with options`() {
        StubFactory(OperationsGrpc.OperationsFutureStub::class, "localhost", 8080, true)
    }

    @Test(expected = IOException::class)
    fun `creates stubs from service account`() {
        val channel: ManagedChannel = mock()
        val factory = StubFactory(OperationsGrpc.OperationsFutureStub::class, channel)

        val creds =
            """
            |{
            |  "type": "service_account",
            |  "client_id": "bar",
            |  "client_email": "foo@example.com",
            |  "private_key_id": "foo",
            |  "private_key": "-"
            |}""".trimMargin()
        try {
            ByteArrayInputStream(creds.toByteArray()).use {
                factory.fromServiceAccount(it, listOf("a", "b"))
            }
        } catch (ex: IOException) {
            assertThat(ex.message).contains("Invalid PKCS#8 data")
            throw ex
        }
    }

    @Test
    fun `creates stubs from access token`() {
        val channel: ManagedChannel = mock()
        val factory = StubFactory(OperationsGrpc.OperationsFutureStub::class, channel)

        val token: AccessToken = mock()
        val stub = factory.fromAccessToken(token, listOf("a", "b"))

        assertThat(stub).isNotNull()
    }

    @Test
    fun `Creates blocking stub from call credentials`() {
        val channel: ManagedChannel = mock()
        val factory = StubFactory(OperationsGrpc.OperationsFutureStub::class, channel)

        val credentials: CallCredentials = mock()
        val stub = factory.fromCallCredentials(credentials)

        assertThat(stub).isNotNull()
        assertThat(stub.options.credentials).isEqualTo(credentials)

        assertThat(factory.channel).isEqualTo(channel)
    }

    @Test
    fun `Creates future stub from call credentials`() {
        val channel: ManagedChannel = mock()
        val factory = StubFactory(OperationsGrpc.OperationsFutureStub::class, channel)

        val credentials: CallCredentials = mock()
        val stub = factory.fromCallCredentials(credentials)

        assertThat(stub).isNotNull()
        assertThat(stub.options.credentials).isEqualTo(credentials)

        assertThat(factory.channel).isEqualTo(channel)
    }

    @Test
    fun `Creates streaming stub from call credentials`() {
        val channel: ManagedChannel = mock()
        val factory = StubFactory(OperationsGrpc.OperationsStub::class, channel)

        val credentials: CallCredentials = mock()
        val stub = factory.fromCallCredentials(credentials)

        assertThat(stub).isNotNull()
        assertThat(stub.options.credentials).isEqualTo(credentials)

        assertThat(factory.channel).isEqualTo(channel)
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
