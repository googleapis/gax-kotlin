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
package com.google.longrunning

import com.google.common.util.concurrent.ListenableFuture
import com.google.protobuf.Empty
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.MethodDescriptor
import io.grpc.MethodDescriptor.generateFullMethodName
import io.grpc.protobuf.lite.ProtoLiteUtils
import io.grpc.stub.AbstractStub
import io.grpc.stub.ClientCalls.futureUnaryCall
import javax.annotation.Generated

/**
 * A low level interface for interacting with Google Long Running Operations.
 *
 * Prefer to use the [com.google.api.kgax.grpc.GrpcClientStub.executeLongRunning] extension.
 */
@Generated("com.google.api.kotlin.generator.GRPCGenerator")
class OperationsClientStub(channel: Channel, callOptions: CallOptions = CallOptions.DEFAULT) :
    AbstractStub<OperationsClientStub>(channel, callOptions) {
    private val listOperationsDescriptor: MethodDescriptor<ListOperationsRequest, ListOperationsResponse> by lazy {
        MethodDescriptor.newBuilder<ListOperationsRequest, ListOperationsResponse>()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(generateFullMethodName("google.longrunning.Operations", "ListOperations"))
            .setSampledToLocalTracing(true)
            .setRequestMarshaller(
                ProtoLiteUtils.marshaller(
                    ListOperationsRequest.getDefaultInstance()
                )
            )
            .setResponseMarshaller(
                ProtoLiteUtils.marshaller(
                    ListOperationsResponse.getDefaultInstance()
                )
            )
            .build()

    }

    private val getOperationDescriptor: MethodDescriptor<GetOperationRequest, Operation> by lazy {
        MethodDescriptor.newBuilder<GetOperationRequest, Operation>()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(generateFullMethodName("google.longrunning.Operations", "GetOperation"))
            .setSampledToLocalTracing(true)
            .setRequestMarshaller(
                ProtoLiteUtils.marshaller(
                    GetOperationRequest.getDefaultInstance()
                )
            )
            .setResponseMarshaller(
                ProtoLiteUtils.marshaller(
                    Operation.getDefaultInstance()
                )
            )
            .build()

    }

    private val deleteOperationDescriptor: MethodDescriptor<DeleteOperationRequest, Empty> by lazy {
        MethodDescriptor.newBuilder<DeleteOperationRequest, Empty>()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(generateFullMethodName("google.longrunning.Operations", "DeleteOperation"))
            .setSampledToLocalTracing(true)
            .setRequestMarshaller(
                ProtoLiteUtils.marshaller(
                    DeleteOperationRequest.getDefaultInstance()
                )
            )
            .setResponseMarshaller(
                ProtoLiteUtils.marshaller(
                    Empty.getDefaultInstance()
                )
            )
            .build()

    }

    private val cancelOperationDescriptor: MethodDescriptor<CancelOperationRequest, Empty> by lazy {
        MethodDescriptor.newBuilder<CancelOperationRequest, Empty>()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(generateFullMethodName("google.longrunning.Operations", "CancelOperation"))
            .setSampledToLocalTracing(true)
            .setRequestMarshaller(
                ProtoLiteUtils.marshaller(
                    CancelOperationRequest.getDefaultInstance()
                )
            )
            .setResponseMarshaller(
                ProtoLiteUtils.marshaller(
                    Empty.getDefaultInstance()
                )
            )
            .build()

    }

    override fun build(channel: Channel, callOptions: CallOptions): OperationsClientStub =
        OperationsClientStub(channel, callOptions)

    fun listOperations(request: ListOperationsRequest): ListenableFuture<ListOperationsResponse> = futureUnaryCall(
        channel.newCall(listOperationsDescriptor, callOptions),
        request
    )

    fun getOperation(request: GetOperationRequest): ListenableFuture<Operation> = futureUnaryCall(
        channel.newCall(getOperationDescriptor, callOptions),
        request
    )

    fun deleteOperation(request: DeleteOperationRequest): ListenableFuture<Empty> = futureUnaryCall(
        channel.newCall(deleteOperationDescriptor, callOptions),
        request
    )

    fun cancelOperation(request: CancelOperationRequest): ListenableFuture<Empty> = futureUnaryCall(
        channel.newCall(cancelOperationDescriptor, callOptions),
        request
    )
}
