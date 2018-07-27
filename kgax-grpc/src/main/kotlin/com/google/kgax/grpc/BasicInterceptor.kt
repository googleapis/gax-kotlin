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

import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.Status

/**
 * Interceptors are an advanced gRPC feature for adding cross cutting logic to your API clients.
 * An interceptor receives the lifecycle events defined in [io.grpc.ClientCall.Listener] and can
 * monitor or change a remote call while it is in progress. This can be used to implement features
 * in your application like auditing all outbounds calls, logging arbitrary data for every inbound
 * or outbound message, counting events, etc.
 *
 * [BasicInterceptor] is a simple interceptor that's useful for monitoring tasks like logging. If
 * you need a more complex interceptor use the gRPC interceptor classes directly to implement it.
 */
open class BasicInterceptor(
    val onReady: () -> Unit = {},
    val onHeaders: (Metadata) -> Unit = {},
    val onMessage: (Any) -> Unit = {},
    val onClose: (Status, Metadata) -> Unit = { _, _ -> }
) : ClientInterceptor {

    override fun <ReqT, RespT> interceptCall(
        method: MethodDescriptor<ReqT, RespT>,
        callOptions: CallOptions,
        next: Channel
    ): ClientCall<ReqT, RespT> {
        val doOnReady = onReady
        val doOnHeaders = onHeaders
        val doOnMessage = onMessage
        val doOnClose = onClose

        return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
            next.newCall(method, callOptions)
        ) {
            override fun start(
                responseListener: ClientCall.Listener<RespT>,
                headers: io.grpc.Metadata
            ) {
                delegate().start(
                    object : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                        responseListener
                    ) {
                        override fun onReady() {
                            try {
                                doOnReady()
                            } finally {
                                super.onReady()
                            }
                        }

                        override fun onHeaders(headers: Metadata) {
                            try {
                                doOnHeaders(headers)
                            } finally {
                                super.onHeaders(headers)
                            }
                        }

                        override fun onMessage(message: RespT) {
                            try {
                                doOnMessage(message as Any)
                            } finally {
                                super.onMessage(message)
                            }
                        }

                        override fun onClose(status: Status, trailers: Metadata) {
                            try {
                                doOnClose(status, trailers)
                            } finally {
                                super.onClose(status, trailers)
                            }
                        }
                    }, headers
                )
            }
        }
    }
}
