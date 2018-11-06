package com.google.longrunning/*
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

import com.google.common.truth.Truth.assertThat
import com.google.common.util.concurrent.ListenableFuture
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.reset
import com.nhaarman.mockito_kotlin.whenever
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.MethodDescriptor
import kotlin.test.Test
import kotlin.test.BeforeTest

class OperationsClientStubTest {

    private val channel: Channel = mock()
    private val callOptions: CallOptions = mock()
    private val call: ClientCall<*, *> = mock()

    @BeforeTest
    fun before() {
        reset(channel, callOptions, call)
        whenever(channel.newCall(any<MethodDescriptor<*, *>>(), eq(callOptions))).doReturn(call)
    }

    private fun newStub() = OperationsClientStub(channel, callOptions)

    private fun <ReqT, RespT> testStub(request: ReqT, func: OperationsClientStub.(ReqT) -> ListenableFuture<RespT>) {
        val stub = newStub()
        val future = stub.func(request)
        assertThat(future).isNotNull()
    }

    @Test
    fun `can list operations`() = testStub(
        ListOperationsRequest.newBuilder().build(),
        OperationsClientStub::listOperations
    )

    @Test
    fun `can get an operation`() = testStub(
        GetOperationRequest.newBuilder().build(),
        OperationsClientStub::getOperation
    )

    @Test
    fun `can delete an operation`() = testStub(
        DeleteOperationRequest.newBuilder().build(),
        OperationsClientStub::deleteOperation
    )

    @Test
    fun `can cancel an operation`() = testStub(
        CancelOperationRequest.newBuilder().build(),
        OperationsClientStub::cancelOperation
    )

}
