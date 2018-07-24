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
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.SettableFuture
import com.google.longrunning.Operation
import com.google.longrunning.OperationsGrpc
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import kotlin.test.Test

class LongRunningCallTest {

    @Test
    fun `LRO waits until done`() {
        val result1: CallResult<Operation> = mock {
            on { body }.thenReturn(Operation.newBuilder().setName("a").setDone(false).build())
        }
        val future1: ListenableFuture<CallResult<Operation>> = mock {
            on { get() }.thenReturn(result1)
        }
        val resultDone: CallResult<Operation> = mock {
            on { body }.thenReturn(Operation.newBuilder().setName("b").setDone(true).build())
        }
        val futureDone: ListenableFuture<CallResult<Operation>> = mock {
            on { get() }.thenReturn(resultDone)
        }
        val grpcClient: GrpcClientStub<OperationsGrpc.OperationsFutureStub> = mock {
            on { executeFuture<Operation>(any()) }
                    .thenReturn(future1)
                    .thenReturn(futureDone)
        }

        val future = SettableFuture.create<CallResult<Operation>>()
        future.set(CallResult(Operation.newBuilder()
                .setName("test_op")
                .setDone(false)
                .build(), mock()))
        val lro = LongRunningCall(grpcClient, future, Operation::class.java)
        val result = lro.waitUntilDone()

        assertThat(result.body).isInstanceOf(Operation::class.java)

        verify(grpcClient, times(2)).executeFuture<Operation>(any())
    }
}