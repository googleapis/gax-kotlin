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
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.check
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.reset
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import kotlin.test.BeforeTest
import kotlin.test.Test

private val TEST_KEY: Metadata.Key<String> =
    Metadata.Key.of("testkey", Metadata.ASCII_STRING_MARSHALLER)
private val TEST_2_KEY: Metadata.Key<String> =
    Metadata.Key.of("anotherkey", Metadata.ASCII_STRING_MARSHALLER)
private val BAD_KEY: Metadata.Key<String> =
    Metadata.Key.of("badKey", Metadata.ASCII_STRING_MARSHALLER)

class GAXInterceptorTest {

    val method: MethodDescriptor<String, String> = mock()
    val clientCall: ClientCall<String, String> = mock()
    val channel: Channel = mock()
    val callOptions: CallOptions = mock()
    val responseListener: ClientCall.Listener<String> = mock()

    @BeforeTest
    fun before() {
        reset(method, clientCall, channel, callOptions, responseListener)
        whenever(channel.newCall(method, callOptions)).doReturn(clientCall)
    }

    @Test
    fun `can intercept headers`() {
        var metadata: Metadata? = null
        val callContext = ClientCallContext(onResponseHeaders = { m -> metadata = m })
        whenever(callOptions.getOption(ClientCallContext.KEY))
            .doReturn(callContext)

        GAXInterceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, mock())

        with(Metadata()) {
            put(TEST_KEY, "this is meta")
            verify(clientCall).start(check {
                it.onHeaders(this)
            }, any())
        }

        assertThat(metadata!!.keys()).containsExactly("testkey")
        assertThat(metadata!!.get(TEST_KEY)).isEqualTo("this is meta")
        assertThat(metadata!!.getAll(TEST_KEY)).containsExactly("this is meta")
    }

    @Test
    fun `can intercept multiple headers`() {
        var metadata: Metadata? = null
        val callContext = ClientCallContext(onResponseHeaders = { m -> metadata = m })
        whenever(callOptions.getOption(ClientCallContext.KEY))
            .doReturn(callContext)

        GAXInterceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, mock())

        with(Metadata()) {
            put(TEST_KEY, "one")
            put(TEST_KEY, "two")
            put(TEST_2_KEY, "three")
            verify(clientCall).start(check {
                it.onHeaders(this)
            }, any())
        }

        assertThat(metadata!!.keys()).containsExactly("testkey", "anotherkey")
        assertThat(metadata!!.get(TEST_KEY)).isEqualTo("two")
        assertThat(metadata!!.getAll(TEST_KEY)).containsExactly("one", "two")
        assertThat(metadata!!.get(TEST_2_KEY)).isEqualTo("three")
    }

    @Test
    fun `does not make up headers`() {
        var metadata: Metadata? = null
        val callContext = ClientCallContext(onResponseHeaders = { m -> metadata = m })
        whenever(callOptions.getOption(ClientCallContext.KEY))
            .doReturn(callContext)

        GAXInterceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, mock())

        with(Metadata()) {
            put(TEST_KEY, "don't use")
            verify(clientCall).start(check {
                it.onHeaders(this)
            }, any())
        }

        assertThat(metadata!!.get(BAD_KEY)).isNull()
        assertThat(metadata!!.getAll(BAD_KEY)).isNull()
    }
}
