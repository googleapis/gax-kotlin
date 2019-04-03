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

import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor

/**
 * This interceptor is always attached to API calls and performs any
 * interceptor functionality required by this library, such as capture response metadata.
 */
internal object GAXInterceptor : ClientInterceptor {

    override fun <ReqT, RespT> interceptCall(
        method: MethodDescriptor<ReqT, RespT>,
        callOptions: CallOptions,
        next: Channel
    ): ClientCall<ReqT, RespT> {
        // init calling context
        val call = next.newCall(method, callOptions)
        callOptions.getOption(ClientCallContext.KEY).call = call

        // start call
        return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(call) {
            override fun start(responseListener: ClientCall.Listener<RespT>, headers: Metadata) {
                delegate().start(
                    object : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                        override fun onHeaders(headers: Metadata?) {
                            try {
                                val opts = callOptions.getOption(ClientCallContext.KEY)
                                if (opts != null && headers != null) {
                                    opts.onResponseHeaders(headers)
                                }
                            } finally {
                                super.onHeaders(headers)
                            }
                        }
                    }, headers
                )
            }
        }
    }
}

/**
 * Content during an ongoing gRPC [call].
 */
class ClientCallContext(
    internal val onResponseHeaders: (Metadata) -> Unit = {}
) {
    /** The remote RPC (this is not set until the call is active). */
    lateinit var call: ClientCall<*, *>

    companion object {
        val KEY: CallOptions.Key<ClientCallContext> = CallOptions.Key.create("kgaxContext")
    }
}
