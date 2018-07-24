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

import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.GoogleCredentials
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.SettableFuture
import com.google.kgax.Page
import com.google.longrunning.Operation
import com.google.longrunning.OperationsGrpc
import com.google.protobuf.MessageLite
import io.grpc.CallCredentials
import io.grpc.ClientInterceptor
import io.grpc.auth.MoreCallCredentials
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import io.grpc.stub.StreamObserver
import java.io.InputStream
import java.util.concurrent.Executor

@DslMarker
annotation class DecoratorMarker

/**
 * A convenience wrapper for gRPC stubs that enables ease of use and the
 * addition of some non-native functionality.
 *
 * You don't typically need to create instance of this class directly. Instead use
 * the [prepare] method that this library defines for all gRPC stubs, which will
 * ensure the [stub] and [options] are setup correctly.
 *
 * For example:
 * ```
 * val stub = StubFactory(MyBlockingStub::class, "host.example.com")
 *                .fromServiceAccount(keyFile, listOf("https://host.example.com/auth/my-scope"))
 * val response = stub.executeBlocking { it -> it.someApiMethod(...) }
 * ```
 */
class GrpcClientStub<T : AbstractStub<T>>(originalStub: T, val options: ClientCallOptions) {

    var stub: T
        private set

    init {
        // add response metadata
        stub = originalStub
                .withInterceptors(ResponseMetadataInterceptor())
                .withOption(ResponseMetadata.KEY, options.responseMetadata)

        // add request metadata
        if (!options.requestMetadata.isEmpty()) {
            val header = io.grpc.Metadata()
            for ((k, v) in options.requestMetadata) {
                val key = io.grpc.Metadata.Key.of(k, io.grpc.Metadata.ASCII_STRING_MARSHALLER)
                v.forEach { header.put(key, it) }
            }
            stub = MetadataUtils.attachHeaders(stub, header)
        }

        // add auth
        if (options.credentials != null) {
            stub = stub.withCallCredentials(options.credentials)
        }

        // add advanced features
        if (options.interceptors.any()) {
            stub = stub.withInterceptors(*options.interceptors.toTypedArray())
        }
    }

    /**
     * Prepare a decorated call to create a [GrpcClientStub]. For example:
     *
     * ```
     * val response = stub.prepare {
     *     withMetadata("foo", listOf("bar"))
     *     withMetadata("1", listOf("a", "b"))
     * }.executeBlocking {
     *     it.myBlockingMethod(...)
     * }
     * print("${response.body}")
     * ```
     *
     * Use this method with the appropriate [GrpcClientStub] method, such as [GrpcClientStub.executeBlocking]
     * instead of calling methods on the gRPC stubs directly when you want to use the additional
     * functionality provided by this library.
     */
    fun prepare(init: ClientCallOptions.Builder.() -> Unit = {}): GrpcClientStub<T> {
        val builder = ClientCallOptions.Builder()
        builder.init()
        return GrpcClientStub(stub, ClientCallOptions(builder))
    }

    /** Prepare a decorated call */
    fun prepare(options: ClientCallOptions) = GrpcClientStub(stub, ClientCallOptions(options))

    /**
     * Execute a blocking call. For example:
     *
     * ```
     * val response = stub.prepare {
     *     withMetadata("foo", listOf("bar"))
     * }.executeBlocking {
     *     it.myBlockingMethod(...)
     * }
     * print("${response.body}")
     * ```
     *
     * The [method] lambda should perform a blocking method call on the stub given as the
     * first parameter. The result along with any additional information, such as
     * [ResponseMetadata], will be returned.
     */
    fun <RespT : MessageLite> executeBlocking(method: (T) -> RespT): CallResult<RespT> {
        val response = method(stub)
        return CallResult(response, options.responseMetadata)
    }

    /**
     * Execute a future-based call. For example:
     *
     * ```
     * val future = stub.prepare {
     *     withMetadata("foo", listOf("bar"))
     * }.executeFuture {
     *     it.myFutureMethod(...)
     * }
     * future.get { print("${it.body}") }
     * ```
     *
     * The [method] lambda should perform a future method call on the stub given as the
     * first parameter. The result along with any additional information, such as
     * [ResponseMetadata], will be returned as a future.
     *
     * Use [ListenableFuture.get] to block for the result or [ListenableFuture.addListener]
     * to access the result asynchronously.
     */
    fun <RespT : MessageLite> executeFuture(
        method: (T) -> ListenableFuture<RespT>
    ): ListenableFuture<CallResult<RespT>> =
            Futures.transform(method(stub)) {
                CallResult(it ?: throw IllegalStateException("Future returned null value"),
                        options.responseMetadata)
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
     */
    fun <RespT : MessageLite> executeLongRunning(
        type: Class<RespT>,
        method: (T) -> ListenableFuture<Operation>
    ): LongRunningCall<RespT> {
        val operationsStub = GrpcClientStub(OperationsGrpc.newFutureStub(stub.channel), options)
        val future = Futures.transform(method(stub)) {
            CallResult(it ?: throw IllegalStateException("Future returned null value"),
                    options.responseMetadata)
        }
        return LongRunningCall(operationsStub, future, type)
    }

    /**
     * Execute a bidirectional streaming call. For example:
     *
     * ```
     * val streams = stub.prepare().executeStreaming { it::myStreamingMethod }
     *
     *  // process incoming responses
     *  streams.responses.onNext = { print("response: $it") }
     *  streams.responses.onError = { print("error: $it") }
     *  streams.responses.onCompleted = { print("all done!") }
     *
     *  // send outbound requests
     *  stream.requests.send(...)
     *  stream.requests.send(...)
     * ```
     *
     * The [method] lambda should return a bound method reference on the stub that is provided
     * as the first parameter to the lambda, as shown in the example. There is no need to call
     * the stub method directly. It will be done as part of this call. The result of this method
     * will provide a pair of inbound and outbound streams.
     *
     * Callbacks attached to the returned stream will be invoked on their original
     * background executor. Set the optional [ResponseStream.executor] parameter as needed
     * (i.e. to have them executed on the main thread, etc.)
     */
    fun <ReqT : MessageLite, RespT : MessageLite> executeStreaming(
        method: (T) -> (StreamObserver<RespT>) -> StreamObserver<ReqT>
    ): StreamingCall<ReqT, RespT> {
        val responseStream = ResponseStreamImpl<RespT>()
        val requestObserver = method(stub)(object : StreamObserver<RespT> {
            override fun onNext(value: RespT) =
                    responseStream.executor?.execute { responseStream.onNext(value) } ?: responseStream.onNext(value)

            override fun onError(t: Throwable) =
                    responseStream.executor?.execute { responseStream.onError(t) } ?: responseStream.onError(t)

            override fun onCompleted() =
                    responseStream.executor?.execute { responseStream.onCompleted() } ?: responseStream.onCompleted()
        })
        val requestStream = object : RequestStream<ReqT> {
            override fun send(request: ReqT) = requestObserver.onNext(request)
        }

        // add and initial requests
        options.initialStreamRequests.map {
            (it as? ReqT) ?: throw IllegalArgumentException("early request data is an invalid type")
        }.forEach { requestStream.send(it) }

        return StreamingCall(requestStream, responseStream)
    }

    /**
     * Execute a call that sends a stream of requests to the server. For example:
     *
     * ```
     * val streams = stub.prepare().executeClientStreaming { it::myStreamingMethod }
     *
     *  // process the response once available
     *  streams.response.get { print("response: $it") }
     *
     *  // send outbound requests
     *  stream.requests.send(...)
     *  stream.requests.send(...)
     * ```
     *
     * The [method] lambda should return a bound method reference on the stub that is provided
     * as the first parameter to the lambda, as shown in the example. There is no need to call
     * the stub method directly. It will be done as part of this call. The result of this method
     * will provide an outbound stream for requests to be sent to the server and a future for
     * the server's response.
     */
    fun <ReqT : MessageLite, RespT : MessageLite> executeClientStreaming(
        method: (T) -> (StreamObserver<RespT>) -> StreamObserver<ReqT>
    ): ClientStreamingCall<ReqT, RespT> {
        val responseFuture = SettableFuture.create<RespT>()
        val requestObserver = method(stub)(object : StreamObserver<RespT> {
            override fun onNext(value: RespT) {
                responseFuture.set(value)
            }

            override fun onError(t: Throwable) {
                responseFuture.setException(t)
            }

            override fun onCompleted() = Unit
        })
        val requestStream = object : RequestStream<ReqT> {
            override fun send(request: ReqT) = requestObserver.onNext(request)
        }

        // add and initial requests
        options.initialStreamRequests.map {
            (it as? ReqT) ?: throw IllegalArgumentException("early request data is an invalid type")
        }.forEach { requestStream.send(it) }

        return ClientStreamingCall(requestStream, responseFuture)
    }

    /**
     * Execute a call that receives one-way streaming responses from the server. For example:
     *
     * ```
     * val request = MyRequest(...)
     * val streams = stub.prepare().executeServerStreaming { it, observer ->
     *   it.myStreamingMethod(request, observer)
     * }
     *
     *  // process incoming responses
     *  streams.responses.onNext = { print("response: $it") }
     *  streams.responses.onError = { print("error: $it") }
     *  streams.responses.onCompleted = { print("all done!") }
     * ```
     *
     * The [method] lambda should call a streaming method reference on the stub that is provided
     * as the first parameter to the lambda using the observer given as the second parameter.
     * The result of this method will provide the inbound stream of responses from the server.
     *
     * Callbacks attached to the returned stream will be invoked on their original
     * background executor. Set the optional [ResponseStream.executor] parameter as needed
     * (i.e. to have them executed on the main thread, etc.)
     */
    fun <RespT : MessageLite> executeServerStreaming(
        method: (T, StreamObserver<RespT>) -> Unit
    ): ServerStreamingCall<RespT> {
        val responseStream = ResponseStreamImpl<RespT>()
        method(stub, object : StreamObserver<RespT> {
            override fun onNext(value: RespT) =
                    responseStream.executor?.execute { responseStream.onNext(value) } ?: responseStream.onNext(value)

            override fun onError(t: Throwable) =
                    responseStream.executor?.execute { responseStream.onError(t) } ?: responseStream.onError(t)

            override fun onCompleted() =
                    responseStream.executor?.execute { responseStream.onCompleted() } ?: responseStream.onCompleted()
        })

        return ServerStreamingCall(responseStream)
    }
}

/** see [GrpcClientStub.prepare] */
fun <T : AbstractStub<T>> T.prepare(init: ClientCallOptions.Builder.() -> Unit = {}): GrpcClientStub<T> {
    val builder = ClientCallOptions.Builder()
    builder.init()
    return GrpcClientStub(this, ClientCallOptions(builder))
}

/** see [GrpcClientStub.prepare] */
fun <T : AbstractStub<T>> T.prepare(options: ClientCallOptions) = GrpcClientStub(this, options)

/**
 * Decorated call options. The settings apply on a per-call level.
 */
class ClientCallOptions constructor(
    val credentials: CallCredentials? = null,
    internal val requestMetadata: Map<String, List<String>> = mapOf(),
    internal val initialStreamRequests: List<Any> = listOf(),
    internal val interceptors: List<ClientInterceptor> = listOf()
) {
    internal val responseMetadata: ResponseMetadata = ResponseMetadata()

    constructor(opts: ClientCallOptions) : this(opts.credentials, opts.requestMetadata,
            opts.initialStreamRequests, opts.interceptors)

    constructor(builder: Builder) : this(builder.credentials, builder.requestMetadata,
            builder.initialStreamRequests, builder.interceptors)

    @DecoratorMarker
    class Builder(
        internal var credentials: CallCredentials? = null,
        internal val requestMetadata: MutableMap<String, List<String>> = mutableMapOf(),
        internal val initialStreamRequests: MutableList<Any> = mutableListOf(),
        internal val interceptors: MutableList<ClientInterceptor> = mutableListOf()
    ) {

        constructor(opts: ClientCallOptions) : this(opts.credentials,
                opts.requestMetadata.toMutableMap(),
                opts.initialStreamRequests.toMutableList(),
                opts.interceptors.toMutableList())

        /** Set service account credentials for authentication */
        fun withServiceAccountCredentials(
            keyFile: InputStream,
            scopes: List<String> = listOf()
        ) {
            val auth = if (scopes.isEmpty()) {
                GoogleCredentials.fromStream(keyFile)
            } else {
                GoogleCredentials.fromStream(keyFile).createScoped(scopes)
            }
            credentials = MoreCallCredentials.from(auth)
        }

        /** Set the access token to use for authentication */
        fun withAccessToken(token: AccessToken, scopes: List<String> = listOf()) {
            val auth = if (scopes.isEmpty()) {
                GoogleCredentials.create(token)
            } else {
                GoogleCredentials.create(token).createScoped(scopes)
            }
            credentials = MoreCallCredentials.from(auth)
        }

        /** Append metadata to the call */
        fun withMetadata(key: String, value: List<String>) {
            requestMetadata[key] = value
        }

        /** Omit metadata from the call */
        fun withoutMetadata(key: String) {
            requestMetadata.remove(key)
        }

        /** For outbound streams, send an initial message as soon as possible */
        fun <T : MessageLite> withInitialRequest(request: T) {
            initialStreamRequests.add(request)
        }

        /** Append arbitrary interceptors (for advanced use) */
        fun withInterceptor(interceptor: ClientInterceptor) {
            interceptors.add(interceptor)
        }

        fun build() = ClientCallOptions(this)
    }
}

/** Result of the call with the response [body] associated [metadata]. */
data class CallResult<RespT>(val body: RespT, val metadata: ResponseMetadata)

/** Result of a call with paging */
data class PageResult<T>(
    override val elements: Iterable<T>,
    override val token: String,
    override val metadata: ResponseMetadata
) : Page<T>

/** A stream of requests to the server. */
interface RequestStream<ReqT> {
    fun send(request: ReqT)
}

/** A stream of responses from the server. */
interface ResponseStream<RespT> {
    var onNext: (RespT) -> Unit
    var onError: (Throwable) -> Unit
    var onCompleted: () -> Unit

    var executor: Executor?
}

/** Result of a bi-directional streaming call including [requests] and [responses] streams. */
data class StreamingCall<ReqT, RespT>(
    val requests: RequestStream<ReqT>,
    val responses: ResponseStream<RespT>
)

/** Result of a client streaming call including the [requests] stream and a [response]. */
data class ClientStreamingCall<ReqT, RespT>(
    val requests: RequestStream<ReqT>,
    val response: ListenableFuture<RespT>
)

/** Result of a server streaming call including the stream of [responses]. */
data class ServerStreamingCall<RespT>(val responses: ResponseStream<RespT>)

/** Result of a server call with the response as a [ListenableFuture]. */
typealias FutureCall<T> = ListenableFuture<CallResult<T>>

/** Add a [callback] that will be run on the provided [executor] when the CallResult is available */
fun <T> FutureCall<T>.enqueue(executor: Executor, callback: (CallResult<T>) -> Unit) =
        this.addListener(java.lang.Runnable {
            callback(this.get() ?: throw IllegalStateException("get() returned an invalid (null) CallResult"))
        }, executor)

private val DIRECT_EXECUTOR = MoreExecutors.directExecutor()

/** Add a [callback] that will be run on the same thread as the caller */
fun <T> FutureCall<T>.enqueue(callback: (CallResult<T>) -> Unit) = DIRECT_EXECUTOR.execute {
    callback(this.get() ?: throw IllegalStateException("get() returned an invalid (null) CallResult"))
}

internal class ResponseStreamImpl<RespT>(
    override var onNext: (RespT) -> Unit = {},
    override var onError: (Throwable) -> Unit = {},
    override var onCompleted: () -> Unit = {},
    override var executor: Executor? = null
) : ResponseStream<RespT>

inline fun <T> ListenableFuture<T>.get(handler: (T) -> Unit) = handler(this.get())
