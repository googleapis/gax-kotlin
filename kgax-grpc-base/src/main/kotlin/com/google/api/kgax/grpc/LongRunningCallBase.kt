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

import com.google.protobuf.MessageLite
import kotlinx.coroutines.Deferred

/** Resolves long running operations. */
abstract class LongRunningCallBase<T : MessageLite, OpT>(
    private val deferred: Deferred<CallResult<OpT>>,
    private val responseType: Class<T>
) {
    /** the underlying operation (null until the operation has completed) */
    var operation: OpT? = null
        protected set

    /** If the operation is done */
    val isDone: Boolean
        get() {
            val op = operation
            return if (op != null) {
                isOperationDone(op)
            } else {
                false
            }
        }

    /** Wait until the operation has been completed. */
    suspend fun await(): CallResult<T> {
        var metadata: ResponseMetadata? = null

        operation = deferred.await().body
        while (!isOperationDone(operation!!)) {
            try {
                // try again
                val result = nextOperation(operation!!)

                // save result of last call
                operation = result.body
                metadata = result.metadata
            } catch (e: InterruptedException) {
                /* ignore and try again */
            }
        }

        return CallResult(
            parse(operation!!, responseType),
            metadata ?: ResponseMetadata()
        )
    }

    protected abstract suspend fun nextOperation(op: OpT): CallResult<OpT>
    protected abstract fun isOperationDone(op: OpT): Boolean
    protected abstract fun parse(operation: OpT, type: Class<T>): T
}
