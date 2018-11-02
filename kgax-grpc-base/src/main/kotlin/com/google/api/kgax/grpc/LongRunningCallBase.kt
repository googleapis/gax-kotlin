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

import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.protobuf.MessageLite
import java.util.concurrent.Callable
import java.util.concurrent.Executor

/** Resolves long running operations. */
abstract class LongRunningCallBase<T : MessageLite, OpT>(
    private val future: ListenableFuture<CallResult<OpT>>,
    private val responseType: Class<T>,
    private val executor: ListeningExecutorService
) {
    /** the underlying operation (null until the operation has completed) */
    var operation: OpT? = null
        protected set

    /** If the operation is done */
    val isDone = future.isDone

    /** Block until the operation has been completed. */
    fun get(): CallResult<T> = asFuture().get()

    /** Add a [callback] that will be run on the provided [executor] when the CallResult is available */
    fun on(executor: Executor, callback: Callback<T>.() -> Unit) =
        asFuture().on(executor, callback)

    /** Get a future that will resolve when the operation has been completed. */
    fun asFuture(): FutureCall<T> = executor.submit(Callable<CallResult<T>> {
        val metadata = wait(future)

        CallResult(
            parse(operation!!, responseType),
            metadata ?: ResponseMetadata()
        )
    })

    /** Block until the operation is complete */
    protected abstract fun wait(future: ListenableFuture<CallResult<OpT>>): ResponseMetadata?

    /** Parse the result of the [operation] to the given [type] or throw an error */
    protected abstract fun parse(operation: OpT, type: Class<T>): T
}
