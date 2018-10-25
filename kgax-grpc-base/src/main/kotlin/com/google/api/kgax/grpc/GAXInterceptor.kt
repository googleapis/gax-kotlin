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
internal class GAXInterceptor : ClientInterceptor {

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
            override fun start(
                responseListener: ClientCall.Listener<RespT>,
                headers: Metadata
            ) {
                delegate().start(
                    object : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                        override fun onHeaders(headers: Metadata?) {
                            val opts = callOptions.getOption(ClientCallContext.KEY)

                            // save a copy of the headers
                            if (headers != null) {
                                val meta = Metadata()
                                meta.merge(headers)
                                opts.responseMetadata.metadata = meta
                            }

                            super.onHeaders(headers)
                        }
                    }, headers
                )
            }
        }
    }
}

/** Content during an ongoing call */
internal class ClientCallContext {
    lateinit var call: ClientCall<*, *>
    val responseMetadata: ResponseMetadata = ResponseMetadata()

    companion object {
        val KEY: CallOptions.Key<ClientCallContext> = CallOptions.Key.create("kgaxContext")
    }
}
