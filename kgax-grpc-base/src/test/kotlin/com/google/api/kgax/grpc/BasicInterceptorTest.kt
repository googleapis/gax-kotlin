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
import com.nhaarman.mockito_kotlin.check
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.reset
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.Status
import kotlin.test.BeforeTest
import kotlin.test.Test

class BasicInterceptorTest {

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
    fun `calls onReady`() {
        val message = "hey"
        val metadata = Metadata()
        var isReady = false

        val interceptor = BasicInterceptor(onReady = { isReady = true })
        interceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, metadata)

        verify(clientCall).start(check {
            simulateCall(it, metadata, message)
        }, eq(metadata))
        assertThat(isReady).isTrue()
    }

    @Test
    fun `calls onClose`() {
        val metadata = Metadata()
        var isClosed = false

        val interceptor = BasicInterceptor(onClose = { _, _ -> isClosed = true })
        interceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, metadata)

        verify(clientCall).start(check {
            simulateCall(it, metadata, "")
        }, eq(metadata))
        assertThat(isClosed).isTrue()
    }

    @Test
    fun `calls onHeaders`() {
        val message = "hey"
        val metadata = Metadata()
        var capturedMetadata: Metadata? = null

        val interceptor = BasicInterceptor(onHeaders = { capturedMetadata = it })
        interceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, metadata)

        verify(clientCall).start(check {
            simulateCall(it, metadata, message)
        }, eq(metadata))
        assertThat(capturedMetadata).isEqualTo(metadata)
    }

    @Test
    fun `calls onMessage`() {
        val message = "hi there"
        val metadata = Metadata()
        var capturedMessage: String? = null

        val interceptor = BasicInterceptor(onMessage = { capturedMessage = it as? String })
        interceptor.interceptCall(method, callOptions, channel)
            .start(responseListener, metadata)

        verify(clientCall).start(check {
            simulateCall(it, metadata, message)
        }, eq(metadata))
        assertThat(capturedMessage).isEqualTo("hi there")
    }

    private fun <T> simulateCall(listener: ClientCall.Listener<T>, metadata: Metadata, message: T) {
        listener.onReady()
        listener.onHeaders(metadata)
        listener.onMessage(message)
        listener.onClose(Status.OK, Metadata())
    }
}