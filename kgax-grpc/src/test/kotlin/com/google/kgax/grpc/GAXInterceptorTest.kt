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

class GAXInterceptorTest {

    val method: MethodDescriptor<String, String> = mock()
    val clientCall: ClientCall<String, String> = mock()
    val channel: Channel = mock()
    val callOptions: CallOptions = mock()
    val responseListener: ClientCall.Listener<String> = mock()

    companion object {
        val TEST_KEY: Metadata.Key<String> =
            Metadata.Key.of("testkey", Metadata.ASCII_STRING_MARSHALLER)
        val TEST_2_KEY: Metadata.Key<String> =
            Metadata.Key.of("anotherkey", Metadata.ASCII_STRING_MARSHALLER)
    }

    @BeforeTest
    fun before() {
        reset(method, clientCall, channel, callOptions, responseListener)
        whenever(channel.newCall(method, callOptions)).doReturn(clientCall)
    }

    @Test
    fun `can intercept headers`() {
        val callContext = ClientCallContext()
        whenever(callOptions.getOption(ClientCallContext.KEY))
            .doReturn(callContext)

        val interceptor = GAXInterceptor()
        interceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, mock())

        val metadata = Metadata()
        metadata.put(TEST_KEY, "this is meta")
        verify(clientCall).start(check {
            it.onHeaders(metadata)
        }, any())

        assertThat(callContext.responseMetadata.keys())
            .containsExactly("testkey")
        assertThat(callContext.responseMetadata.get("testkey"))
            .isEqualTo("this is meta")
        assertThat(callContext.responseMetadata.getAll("testKey"))
            .containsExactly("this is meta")
    }

    @Test
    fun `can intercept multiple headers`() {
        val callContext = ClientCallContext()
        whenever(callOptions.getOption(ClientCallContext.KEY))
            .doReturn(callContext)

        val interceptor = GAXInterceptor()
        interceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, mock())

        val metadata = Metadata()
        metadata.put(TEST_KEY, "one")
        metadata.put(TEST_KEY, "two")
        metadata.put(TEST_2_KEY, "three")
        verify(clientCall).start(check {
            it.onHeaders(metadata)
        }, any())

        assertThat(callContext.responseMetadata.keys())
            .containsExactly("testkey", "anotherkey")
        assertThat(callContext.responseMetadata.get("testkey"))
            .isEqualTo("two")
        assertThat(callContext.responseMetadata.getAll("testKey"))
            .containsExactly("one", "two")
        assertThat(callContext.responseMetadata.get("anotherkey"))
            .isEqualTo("three")
    }

    @Test
    fun `does not make up headers`() {
        val callContext = ClientCallContext()
        whenever(callOptions.getOption(ClientCallContext.KEY))
            .doReturn(callContext)

        val interceptor = GAXInterceptor()
        interceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, mock())

        val metadata = Metadata()
        metadata.put(TEST_KEY, "don't use")
        verify(clientCall).start(check {
            it.onHeaders(metadata)
        }, any())

        assertThat(callContext.responseMetadata.get("wrong")).isNull()
        assertThat(callContext.responseMetadata.getAll("wrong")).isNull()
    }

    @Test
    fun `starts with null headers`() {
        val callContext = ClientCallContext()
        whenever(callOptions.getOption(ClientCallContext.KEY))
            .doReturn(callContext)

        val interceptor = GAXInterceptor()
        interceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, mock())

        assertThat(callContext.responseMetadata.metadata).isNull()
        assertThat(callContext.responseMetadata.keys()).isEmpty()
        assertThat(callContext.responseMetadata.get(TEST_KEY.name())).isNull()
        assertThat(callContext.responseMetadata.getAll(TEST_KEY.name())).isNull()
    }
}
