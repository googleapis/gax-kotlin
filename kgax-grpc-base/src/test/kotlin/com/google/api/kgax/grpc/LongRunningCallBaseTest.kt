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
import com.google.protobuf.StringValue
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertFailsWith

private fun string(value: String): StringValue = StringValue.newBuilder().setValue(value).build()

class LongRunningCallBaseTest {

    @Test
    fun `LRO is done`() = runBlocking {
        val lro = LRO(GlobalScope.async {
            OpClass(1, done = true, value = string("1"))
        })
        assertThat(lro.isDone).isFalse()
        lro.await()
        assertThat(lro.isDone).isTrue()
    }

    @Test
    fun `LRO is not done`() {
        assertFailsWith<java.lang.RuntimeException>("did not complete") {
            runBlocking {
                val lro = LRO(GlobalScope.async {
                    OpClass(1, done = false, value = string("1"))
                }, false)
                assertThat(lro.isDone).isFalse()
                lro.await()
            }
        }
    }
}

private class OpClass(val id: Int, val done: Boolean = false, val value: StringValue? = null)

private class LRO(
    deferred: Deferred<OpClass>,
    val willComplete: Boolean = true
) : LongRunningCallBase<StringValue, OpClass>(deferred, StringValue::class.java) {
    override suspend fun nextOperation(op: OpClass): OpClass {
        return if (op.id < 10) {
            OpClass(op.id + 1)
        } else {
            if (willComplete) {
                OpClass(op.id + 1, done = true, value = string("done!"))
            } else {
                throw RuntimeException("did not complete")
            }
        }
    }

    override fun isOperationDone(op: OpClass) = op.done

    override fun parse(operation: OpClass, type: Class<StringValue>): StringValue {
        return operation.value ?: throw RuntimeException("no value")
    }
}
