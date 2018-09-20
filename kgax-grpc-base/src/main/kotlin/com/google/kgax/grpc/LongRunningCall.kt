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

import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors
import com.google.longrunning.GetOperationRequest
import com.google.longrunning.Operation
import com.google.longrunning.OperationsGrpc
import com.google.protobuf.ByteString
import com.google.protobuf.MessageLite
import io.grpc.Status
import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.Executors

/** Resolves long running operations. */
class LongRunningCall<T : MessageLite>(
    private val stub: GrpcClientStub<OperationsGrpc.OperationsFutureStub>,
    private val future: ListenableFuture<CallResult<Operation>>,
    private val responseType: Class<T>,
    private val executor: ListeningExecutorService = LongRunningCall.executor
) {

    /** the underlying operation (null until the operation has completed) */
    var operation: Operation? = null
        private set

    /** If the operation is done */
    val isDone = future.isDone

    internal fun waitUntilDone(): CallResult<T> {
        operation = future.get().body
        while (!operation!!.done) {
            try {
                operation = stub.executeFuture {
                    it.getOperation(
                        GetOperationRequest.newBuilder()
                            .setName(operation!!.name)
                            .build()
                    )
                }.get().body
            } catch (e: InterruptedException) {
                /* ignore and try again */
            }
        }
        // TODO: get actual metadata
        return CallResult(parseResult(operation!!, responseType), ResponseMetadata())
    }

    /** Block until the operation has been completed. */
    fun get() = asFuture().get()

    /** Add a [callback] that will be run on the provided [executor] when the CallResult is available */
    fun on(executor: Executor, callback: Callback<T>.() -> Unit) =
        asFuture().on(executor, callback)

    /** Get a future that will resolve when the operation has been completed. */
    fun asFuture(): FutureCall<T> = executor.submit(Callable<CallResult<T>> { waitUntilDone() })

    /** Parse the result of the [op] to the given [type] or throw an error */
    private fun <T : MessageLite> parseResult(op: Operation, type: Class<T>): T {
        if (op.error == null || op.error.code == Status.Code.OK.value()) {
            @Suppress("UNCHECKED_CAST")
            return type.getMethod(
                "parseFrom",
                ByteString::class.java
            ).invoke(null, op.response.value) as T
        }

        throw RuntimeException("Operation completed with error: ${op.error.code}\n details: ${op.error.message}")
    }

    companion object {
        /** The executor to use for resolving operations/ */
        var executor: ListeningExecutorService = MoreExecutors.listeningDecorator(
            Executors.newCachedThreadPool()
        )
    }
}
