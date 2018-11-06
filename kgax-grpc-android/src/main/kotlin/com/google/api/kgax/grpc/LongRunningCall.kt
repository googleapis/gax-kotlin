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

import com.google.api.kgax.RetryContext
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.SettableFuture
import com.google.longrunning.GetOperationRequest
import com.google.longrunning.Operation
import com.google.longrunning.OperationsClientStub
import com.google.protobuf.ByteString
import com.google.protobuf.MessageLite
import io.grpc.Status
import io.grpc.stub.AbstractStub
import java.util.concurrent.Executors

/** Resolves long running operations. */
class LongRunningCall<T : MessageLite>(
    private val stub: GrpcClientStub<OperationsClientStub>,
    future: ListenableFuture<CallResult<Operation>>,
    responseType: Class<T>,
    executor: ListeningExecutorService = Companion.executor
) : LongRunningCallBase<T, Operation>(future, responseType, executor) {

    companion object {
        /** The default executor to use for resolving operations. */
        var executor: ListeningExecutorService = MoreExecutors.listeningDecorator(
            Executors.newCachedThreadPool()
        )
    }

    override fun isOperationDone(op: Operation) = op.done

    override fun nextOperation(op: Operation) = stub.executeFuture {
        it.getOperation(
            GetOperationRequest.newBuilder()
                .setName(operation!!.name)
                .build()
        )
    }.get()!!

    override fun parse(operation: Operation, type: Class<T>): T {
        if (operation.error == null || operation.error.code == Status.Code.OK.value()) {
            @Suppress("UNCHECKED_CAST")
            return type.getMethod(
                "parseFrom",
                ByteString::class.java
            ).invoke(null, operation.response.value) as T
        }

        throw RuntimeException("Operation completed with error: ${operation.error.code}\n details: ${operation.error.message}")
    }
}

/**
 * Execute a long running operation. For example:
 *
 * ```
 * val lro = stub.executeLongRunning(MyLongRunningResponse::class.java) {
 *     it.myLongRunningMethod(...)
 * }
 * lro.get { print("${it.body}") }
 * ```
 *
 * The [method] lambda should perform a future method call on the stub given as the
 * first parameter. The result along with any additional information, such as
 * [ResponseMetadata], will be returned as a [LongRunningCall]. The [type] given
 * must match the return type of the Operation.
 *
 * An optional [context] can be supplied to enable arbitrary retry strategies.
 */
fun <RespT : MessageLite, T : AbstractStub<T>> GrpcClientStub<T>.executeLongRunning(
    type: Class<RespT>,
    context: String = "",
    method: (T) -> ListenableFuture<Operation>
): LongRunningCall<RespT> {
    val operationsStub = GrpcClientStub(OperationsClientStub(stubWithContext().channel), options)
    val future: SettableFuture<CallResult<Operation>> = SettableFuture.create()
    executeFuture(method, future, RetryContext(context))
    return LongRunningCall(operationsStub, future, type)
}
