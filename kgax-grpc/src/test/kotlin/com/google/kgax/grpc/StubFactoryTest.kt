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
import io.grpc.CallCredentials
import kotlin.test.Test

class StubFactoryTest {

    @Test
    fun `Creates blocking stub from call credentials`() {
        val factory = StubFactory(OperationsGrpc.OperationsBlockingStub::class, "foo.example.com", 443, true)

        val credentials: CallCredentials = mock()
        val stub = factory.fromCallCredentials(credentials)

        assertThat(stub).isNotNull()
        assertThat(stub.callOptions.credentials).isEqualTo(credentials)
    }

    @Test
    fun `Creates future stub from call credentials`() {
        val factory = StubFactory(OperationsGrpc.OperationsFutureStub::class, "foo.example.com", 443, true)

        val credentials: CallCredentials = mock()
        val stub = factory.fromCallCredentials(credentials)

        assertThat(stub).isNotNull()
        assertThat(stub.callOptions.credentials).isEqualTo(credentials)
    }

    @Test
    fun `Creates streaming stub from call credentials`() {
        val factory = StubFactory(OperationsGrpc.OperationsStub::class, "foo.example.com", 443, true)

        val credentials: CallCredentials = mock()
        val stub = factory.fromCallCredentials(credentials)

        assertThat(stub).isNotNull()
        assertThat(stub.callOptions.credentials).isEqualTo(credentials)
    }

}