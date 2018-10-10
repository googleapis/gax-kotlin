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
import io.grpc.stub.AbstractStub
import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertFailsWith

class StubFactoryTest {

    @Test
    fun `creates stubs from factory`() {
        val factory = StubFactory(OperationsGrpc.OperationsFutureStub::class, "localhost")
        assertThat(factory.channel).isNotNull()
        factory.channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
    }

    @Test
    fun `creates stubs from factory with options`() {
        val factory = StubFactory(OperationsGrpc.OperationsFutureStub::class, "localhost", 8080, true)
        assertThat(factory.channel).isNotNull()
        factory.channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
    }

    @Test
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
            |  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCn72FRF4rychJw8qwt3OjfPZq/fCuNBspdlaWtC3wtqn5ft+3dzeDVwhHsGoQ00i+bsKeKRdN/7IMr1NUofFwQi04gXP4Z5JMot0fVXT06oQR/9sXPXN8Y/g9oo6pDkt2Pt9oJyPxLoVVyenU1K2qQDZbu3Vht3CNC4M5hlVxWPz17wjaysZTYbQ67uRhq16lLf1azOr5qU/jbpQQNfrABa+WOnOPGUcBokJGkLYJ4sqlvxKScCydyGnp+NtkaDM7bh4vtXPcwvWGnMOlBls835BRu4atO2r2IaTYBdOrZBbxRJ7XD9NX9Xmx2izUP1pjQRyr+gEsh4iCDe/ihD2JnAgMBAAECggEAU3A6mAHshX1r8DaCmd7yzUhchNt6//zpuwnJYWsdbcsSMxi91MCf/1UHdpnOKjhscxyYgbkj1qF6ouKCbjUrwQSV8rqBfff1rfsyc65555FtHO9NZTQtnMtOeJ0o0Z48+1VLviTgReuI+vIhHiPONHisD05my17fHjF4GUAOusKuhGGjs9FAYuooLvWgYFQz/C7UWCDPl9087nTVz9h63Ysox5OGsOPXHOC54L5su1wuuQNDUucFToJctxE0SVeHVpSbrzdgVJjwN0qIi6zQIUzwz2t6OUhA0SIi66YTL9qYf+E21IKNqPscIQw81WfZpIqU605N/XUo83tN3cdtgQKBgQDe1wCbqa+GQ5oJF7KI1kMW3s1SBvGIz2XhrLLA4xw/yk1P9uWsbEVV20AA3wJZHDntoQ47M0M504pcMS3MknwisT+7NZptBX7BCJcendcbiUlLt0jvSFg0Crkiyw2khVNTwihq6CRnV3KnQRldDzKm3P5x/Ifa/jmQ5IOUHzJeIQKBgQDA7M09l7XPdunMGS9+yaTP7un1pjEXWOIiWYdCa9MsOaab3FUIzk501GtSUbjCSwbUU8iaAqrmfAUwQxUxXcHBzOGbsshohSTFYU2kac+94sykvPjPXBtXNf7JyKip5BMzoNydoZPw9JDMlP7jO20vuev0ymiSAqUhSBwXQ2HfhwKBgCeMQshdihArCThZ406TsB5r9kZ7gvxDypINoz/GTqonjicF62b5ZCjDm41MBs+nycQZlDv/cgveNiz8cWNgD/XcPTJNZhW5JvC9RIyjeJyjdcWhRqloznaV/JtnLAmpu8sepyup/WP1yhxS2lyAqP2iNOon5jiAa9kCJTPxgW/hAoGAP+KqnEjOteLEzQdSCQQxBYIyC1x7SSXvzDwlZENcbHqyx04RApd+t4VX/Kx/KCe8HTeZaBaWDTjoZvDv5acMcGauFub/Ik1kvc/Y7Cb12gVuiubg5Zm0nA6PTraZ05hpG2GbbL+Cw/nYsUZtmUWmhHVmw2r7cc5abEj6tGFl2aUCgYBntW5CQH6H0N/vJvRuF+qwF5AEImPmlwrFmgJFy0+q3o7TaV45zpg3ndTBGM0S4P0bAkS/HseFCY7qsOE4jQem90OUr/RBJGDgFcm5yOlVRVr8fezdJyBSO5LCZ16yUxly6WAJLXILxmGZ1KUKZPtyYlsOrTPJFjfYZkOxYZCU5w==\n-----END PRIVATE KEY-----\n"
            |}""".trimMargin()
        val stub = ByteArrayInputStream(creds.toByteArray()).use {
            factory.fromServiceAccount(it, listOf("a", "b"))
        }

        assertThat(stub).isNotNull()
        assertThat(stub.originalStub).isInstanceOf(OperationsGrpc.OperationsFutureStub::class.java)
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
