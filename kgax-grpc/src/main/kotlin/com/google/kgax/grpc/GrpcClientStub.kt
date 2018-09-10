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
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
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
class GrpcClientStub<T : AbstractStub<T>>(val originalStub: T, val options: ClientCallOptions) {

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
        val builder = ClientCallOptions.Builder(options)
        builder.init()
        return GrpcClientStub(originalStub, ClientCallOptions(builder))
    }

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
        val stub = stubWithContext()
        val response = method(stub)
        return CallResult(response, stub.context.responseMetadata)
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
    ): ListenableFuture<CallResult<RespT>> {
        val stub = stubWithContext()
        return Futures.transform(method(stub)) {
            CallResult(
                it ?: throw IllegalStateException("Future returned null value"),
                stub.context.responseMetadata
            )
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
     */
    fun <RespT : MessageLite> executeLongRunning(
        type: Class<RespT>,
        method: (T) -> ListenableFuture<Operation>
    ): LongRunningCall<RespT> {
        val stub = stubWithContext()
        val operationsStub = GrpcClientStub(OperationsGrpc.newFutureStub(stub.channel), options)
        val future = Futures.transform(method(stub)) {
            CallResult(
                it ?: throw IllegalStateException("Future returned null value"),
                stub.context.responseMetadata
            )
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
        val stub = stubWithContext()

        // starts the call
        val doStart = { responseStream: ResponseStreamImpl<RespT> ->
            // handle requests
            val requestObserver = method(stub)(object : StreamObserver<RespT> {
                override fun onNext(value: RespT) =
                    responseStream.executor?.execute { responseStream.onNext(value) }
                        ?: responseStream.onNext(value)

                override fun onError(t: Throwable) =
                    responseStream.executor?.execute { responseStream.onError(t) }
                        ?: responseStream.onError(t)

                override fun onCompleted() =
                    responseStream.executor?.execute { responseStream.onCompleted() }
                        ?: responseStream.onCompleted()
            })
            val requestStream: RequestStream<ReqT> = object : RequestStream<ReqT> {
                override fun send(request: ReqT) = requestObserver.onNext(request)
                override fun close() = requestObserver.onCompleted()
            }

            // add and initial requests
            for (request in options.initialRequests) {
                @Suppress("UNCHECKED_CAST")
                requestStream.send(request as ReqT)
            }

            requestStream
        }

        // handle responses
        val responseStream = object : ResponseStreamImpl<RespT>() {
            override fun close() {
                stub.context.call.cancel(
                        "explicit close() called by client",
                        StreamingMethodClosedException()
                )
            }
        }

        return StreamingCallImpl(doStart, responseStream)
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
        val stub = stubWithContext()

        // starts the call
        val doStart = { responseFuture: SettableFuture<RespT> ->
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
                override fun close() = requestObserver.onCompleted()
            }

            // add and initial requests
            for (request in options.initialRequests) {
                @Suppress("UNCHECKED_CAST")
                requestStream.send(request as ReqT)
            }

            requestStream
        }

        return ClientStreamingCallImpl(doStart, SettableFuture.create<RespT>())
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
        val stub = stubWithContext()

        // starts the call
        val doStart = { responseStream: ResponseStreamImpl<RespT> ->
            method(stub, object : StreamObserver<RespT> {
                override fun onNext(value: RespT) =
                    responseStream.executor?.execute { responseStream.onNext(value) }
                        ?: responseStream.onNext(value)

                override fun onError(t: Throwable) =
                    responseStream.executor?.execute { responseStream.onError(t) }
                        ?: responseStream.onError(t)

                override fun onCompleted() =
                    responseStream.executor?.execute { responseStream.onCompleted() }
                        ?: responseStream.onCompleted()
            })
        }

        val responseStream = object : ResponseStreamImpl<RespT>() {
            override fun close() {
                stub.context.call.cancel(
                        "explicit close() called by client",
                        StreamingMethodClosedException()
                )
            }
        }

        return ServerStreamingCallImpl(doStart, responseStream)
    }

    /**
     * Gets a one-time use stub with an initial (empty) context.
     *
     * This should be called before each API method call.
     */
    private fun stubWithContext(): T {
        // add gax interceptor
        var stub = originalStub
            .withInterceptors(GAXInterceptor())
            .withOption(ClientCallContext.KEY, ClientCallContext())

        // add request metadata
        if (options.requestMetadata.isNotEmpty()) {
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

        return stub
    }
}

/** Indicates the user explictly closed the connection */
class StreamingMethodClosedException : Exception()

/** Get the call context associated with a one time use stub */
private val <T : AbstractStub<T>> T.context: ClientCallContext
    get() = this.callOptions.getOption(ClientCallContext.KEY)

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
    val requestMetadata: Map<String, List<String>> = mapOf(),
    val initialRequests: List<Any> = listOf(),
    val interceptors: List<ClientInterceptor> = listOf()
) {

    constructor(builder: Builder) : this(
        builder.credentials, builder.requestMetadata,
        builder.initialStreamRequests, builder.interceptors
    )

    @DecoratorMarker
    class Builder(
        internal var credentials: CallCredentials? = null,
        internal val requestMetadata: MutableMap<String, List<String>> = mutableMapOf(),
        internal val initialStreamRequests: MutableList<Any> = mutableListOf(),
        internal val interceptors: MutableList<ClientInterceptor> = mutableListOf()
    ) {

        constructor(opts: ClientCallOptions) : this(
            opts.credentials,
            opts.requestMetadata.toMutableMap(),
            opts.initialRequests.toMutableList(),
            opts.interceptors.toMutableList()
        )

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

internal fun clientCallOptions(init: ClientCallOptions.Builder.() -> Unit = {}): ClientCallOptions {
    val builder = ClientCallOptions.Builder()
    builder.apply(init)
    return builder.build()
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
interface RequestStream<ReqT> : AutoCloseable {
    /**
     * Send the next request to the server.
     *
     * Cannot be called after [close].
     */
    fun send(request: ReqT)
}

/** A stream of responses from the server. */
interface ResponseStream<RespT> : AutoCloseable {
    var onNext: (RespT) -> Unit
    var onError: (Throwable) -> Unit
    var onCompleted: () -> Unit

    var executor: Executor?
}

/** [ResponseStream] with defaults */
internal abstract class ResponseStreamImpl<RespT>(
    override var onNext: (RespT) -> Unit = {},
    override var onError: (Throwable) -> Unit = {},
    override var onCompleted: () -> Unit = {},
    override var executor: Executor? = null
) : ResponseStream<RespT>

/**
 * Result of a bi-directional streaming call including [requests] and [responses] streams.
 *
 * The [start] method must be called before accessing [requests] or [responses] and
 * should be called only once.
 */
interface StreamingCall<ReqT, RespT> {
    val requests: RequestStream<ReqT>
    val responses: ResponseStream<RespT>

    fun start(init: (ResponseStream<RespT>.() -> Unit)? = null)
}

/**
 * Result of a client streaming call including the [requests] stream and a [response].
 *
 * The [start] method must be called before accessing [requests] or [response] and
 * should be called only once.
 */
interface ClientStreamingCall<ReqT, RespT> {
    val requests: RequestStream<ReqT>
    val response: ListenableFuture<RespT>

    fun start()
}

/**
 * Result of a server streaming call including the stream of [responses].
 *
 * The [start] method must be called before accessing [responses] and
 * should be called only once.
 */
interface ServerStreamingCall<RespT> {
    val responses: ResponseStream<RespT>

    fun start(init: (ResponseStream<RespT>.() -> Unit)?)
}

internal class StreamingCallImpl<ReqT, RespT>(
    private val start: (ResponseStreamImpl<RespT>) -> RequestStream<ReqT>,
    private val responseStream: ResponseStreamImpl<RespT>
) : StreamingCall<ReqT, RespT> {

    override lateinit var requests: RequestStream<ReqT>
    override var responses: ResponseStream<RespT> = responseStream

    override fun start(init: (ResponseStream<RespT>.() -> Unit)?) {
        init?.let { responses.apply(it) }
        requests = start(responseStream)
    }
}

internal class ClientStreamingCallImpl<ReqT, RespT>(
    private val start: (SettableFuture<RespT>) -> RequestStream<ReqT>,
    private val responseFuture: SettableFuture<RespT>
) : ClientStreamingCall<ReqT, RespT> {

    override lateinit var requests: RequestStream<ReqT>
    override var response: ListenableFuture<RespT> = responseFuture

    override fun start() {
        requests = start(responseFuture)
    }
}

internal class ServerStreamingCallImpl<RespT>(
    private val start: (ResponseStreamImpl<RespT>) -> Unit,
    private val responseStream: ResponseStreamImpl<RespT>
) : ServerStreamingCall<RespT> {

    override var responses: ResponseStream<RespT> = responseStream

    override fun start(init: (ResponseStream<RespT>.() -> Unit)?) {
        init?.let { responses.apply(it) }
        start(responseStream)
    }
}

/** Result of a server call with the response as a [ListenableFuture]. */
typealias FutureCall<T> = ListenableFuture<CallResult<T>>

@DecoratorMarker
class Callback<T> {
    var success: (CallResult<T>) -> Unit = {}
    var error: (Throwable) -> Unit = {}
    var ignoreIf: (() -> Boolean)? = null
    var ignoreResultIf: ((CallResult<T>) -> Boolean)? = null
    var ignoreErrorIf: ((Throwable) -> Boolean)? = null
}

/** Add a [callback] that will be run on the provided [executor] when the CallResult is available */
fun <T> FutureCall<T>.on(executor: Executor, callback: Callback<T>.() -> Unit) {
    Futures.addCallback<CallResult<T>>(this, object : FutureCallback<CallResult<T>> {
        val cb = Callback<T>().apply(callback)

        override fun onSuccess(result: CallResult<T>?) {
            val ignoreAll = cb.ignoreIf?.let { it() } ?: false
            val ignore = cb.ignoreResultIf?.let { it(result!!) } ?: false

            if (!ignoreAll && !ignore) {
                cb.success(result!!)
            }
        }

        override fun onFailure(t: Throwable?) {
            val ignoreAll = cb.ignoreIf?.let { it() } ?: false
            val ignore = cb.ignoreErrorIf?.let { it(t!!) } ?: false

            if (!ignoreAll && !ignore) {
                cb.error(t!!)
            }
        }
    }, executor)
}

/** Get the result of the future */
inline fun <T> ListenableFuture<T>.get(handler: (T) -> Unit) = handler(this.get())
