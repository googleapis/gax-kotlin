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
import com.google.kgax.NoRetry
import com.google.kgax.Page
import com.google.kgax.Retry
import com.google.kgax.RetryContext
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
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.Executor

@DslMarker
annotation class DecoratorMarker

// timer used for retries
private val RETRY_TIMER = Timer()

/**
 * A convenience wrapper for gRPC stubs that enables ease of use and the
 * addition of some non-native functionality.
 *
 * You don't typically need to create instance of this class directly. Instead use
 * the [prepare] method that this library defines for all gRPC stubs, which will
 * ensure the [originalStub] and [options] are setup correctly.
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
    fun <RespT : MessageLite> executeBlocking(method: (T) -> RespT) = executeBlocking(method, RetryContext())

    private fun <RespT : MessageLite> executeBlocking(
        method: (T) -> RespT,
        retryContext: RetryContext
    ): CallResult<RespT> {
        try {
            val stub = stubWithContext()
            val response = method(stub)
            return CallResult(response, stub.context.responseMetadata)
        } catch (t: Throwable) {
            val retryAfter = options.retry.retryAfter(t, retryContext)
            if (retryAfter != null) {
                try {
                    Thread.sleep(retryAfter)
                } catch (int: InterruptedException) {
                    // ignore
                }
                return executeBlocking(method, retryContext.next())
            } else {
                throw t
            }
        }
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
        val future: SettableFuture<CallResult<RespT>> = SettableFuture.create()
        executeFuture(method, future, RetryContext())
        return future
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
        val operationsStub = GrpcClientStub(OperationsGrpc.newFutureStub(stubWithContext().channel), options)
        val future: SettableFuture<CallResult<Operation>> = SettableFuture.create()
        executeFuture(method, future, RetryContext())
        return LongRunningCall(operationsStub, future, type)
    }

    private fun <RespT : MessageLite> executeFuture(
        method: (T) -> ListenableFuture<RespT>,
        resultFuture: SettableFuture<CallResult<RespT>>,
        retryContext: RetryContext
    ) {
        val stub = stubWithContext()

        Futures.addCallback(method(stub), object : FutureCallback<RespT> {
            override fun onSuccess(result: RespT?) {
                if (result != null) {
                    resultFuture.set(CallResult(result, stub.context.responseMetadata))
                } else {
                    resultFuture.setException(IllegalStateException("Future returned null value"))
                }
            }

            override fun onFailure(t: Throwable) {
                val retryAfter = options.retry.retryAfter(t, retryContext)
                if (retryAfter != null) {
                    RETRY_TIMER.schedule(object : TimerTask() {
                        override fun run() = executeFuture(method, resultFuture, retryContext.next())
                    }, retryAfter)
                } else {
                    resultFuture.setException(t)
                }
            }
        })
    }

    /**
     * Execute a bidirectional streaming call. For example:
     *
     * ```
     * val stream = stub.prepare().executeStreaming { it::myStreamingMethod }
     *
     * // process incoming responses
     * stream.start {
     *     onNext = { print("response: $it") }
     *     onError = { print("error: $it") }
     *     onCompleted = { print("all done!") }
     * }
     *
     * // send outbound requests
     * stream.requests.send(...)
     * stream.requests.send(...)
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
        // starts the call
        val doStart = { self: StreamingCallImpl<ReqT, RespT>, retryContext: RetryContext ->
            val stub = stubWithContext()

            // handle responses
            val responseStream = object : ResponseStreamImpl<RespT>() {
                override fun close() {
                    stub.context.call.cancel(
                        "explicit close() called by client",
                        StreamingMethodClosedException()
                    )
                }
            }

            // handle requests
            val responseStreamObserver = createResponseStreamObserver(responseStream, retryContext) {
                self.restart(it.next())
            }
            val requestObserver = method(stub)(responseStreamObserver)
            val requestStream: RequestStream<ReqT> = object : RequestStream<ReqT> {
                override fun send(request: ReqT) = requestObserver.onNext(request)
                override fun close() = requestObserver.onCompleted()
            }

            // add and initial requests
            for (request in options.initialRequests) {
                @Suppress("UNCHECKED_CAST")
                requestStream.send(request as ReqT)
            }

            Pair(requestStream, responseStream)
        }

        return StreamingCallImpl(doStart)
    }

    /**
     * Execute a call that sends a stream of requests to the server. For example:
     *
     * ```
     * val stream = stub.prepare().executeClientStreaming { it::myStreamingMethod }
     *
     * // send outbound requests
     * stream.requests.send(...)
     * stream.requests.send(...)
     *
     * // process the response once available
     * stream.response.get { print("response: $it") }
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
        // starts the call
        val doStart = { self: ClientStreamingCallImpl<ReqT, RespT>, retryContext: RetryContext ->
            val stub = stubWithContext()
            var canRetry = true

            val requestObserver = method(stub)(object : StreamObserver<RespT> {
                override fun onNext(value: RespT) {
                    canRetry = false
                    self.response.set(value)
                }

                override fun onError(t: Throwable) {
                    try {
                        val retryAfter = if (canRetry) options.retry.retryAfter(t, retryContext) else null
                        if (retryAfter != null) {
                            RETRY_TIMER.schedule(object : TimerTask() {
                                override fun run() = self.restart(retryContext.next())
                            }, retryAfter)
                        } else {
                            self.response.setException(t)
                        }
                    } finally {
                        canRetry = false
                    }
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

        return ClientStreamingCallImpl(doStart)
    }

    /**
     * Execute a call that receives one-way streaming responses from the server. For example:
     *
     * ```
     * val request = MyRequest(...)
     * val stream = stub.prepare().executeServerStreaming { it, observer ->
     *   it.myStreamingMethod(request, observer)
     * }
     *
     * // process incoming responses
     * stream.start {
     *     onNext = { print("response: $it") }
     *     onError = { print("error: $it") }
     *     onCompleted = { print("all done!") }
     * }
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
        // starts the call
        val doStart = { self: ServerStreamingCallImpl<RespT>, retryContext: RetryContext ->
            val stub = stubWithContext()

            // handle responses
            val responseStream = object : ResponseStreamImpl<RespT>() {
                override fun close() {
                    stub.context.call.cancel(
                        "explicit close() called by client",
                        StreamingMethodClosedException()
                    )
                }
            }

            // make request
            val responseStreamObserver = createResponseStreamObserver(responseStream, retryContext) {
                self.restart(it.next())
            }
            method(stub, responseStreamObserver)

            responseStream
        }

        return ServerStreamingCallImpl(doStart)
    }

    // helper for handling streaming responses from the server
    private fun <RespT : MessageLite> createResponseStreamObserver(
        responseStream: ResponseStreamImpl<RespT>,
        retryContext: RetryContext,
        restart: (retryContext: RetryContext) -> Unit
    ): StreamObserver<RespT> {
        return object : StreamObserver<RespT> {
            var canRetry = true

            override fun onNext(value: RespT) {
                canRetry = false
                if (!ignore(responseStream.ignoreIf(), responseStream.ignoreNextIf(value))) {
                    responseStream.executor?.execute { responseStream.onNext(value) }
                        ?: responseStream.onNext(value)
                }
            }

            override fun onError(t: Throwable) {
                try {
                    if (canRetry) {
                        val retryAfter = options.retry.retryAfter(t, retryContext)
                        if (retryAfter != null) {
                            RETRY_TIMER.schedule(object : TimerTask() {
                                override fun run() = restart(retryContext)
                            }, retryAfter)
                            return
                        }
                    }
                } finally {
                    canRetry = false
                }

                if (!ignore(responseStream.ignoreIf(), responseStream.ignoreErrorIf(t))) {
                    responseStream.executor?.execute { responseStream.onError(t) }
                        ?: responseStream.onError(t)
                }
            }

            override fun onCompleted() {
                canRetry = false
                if (!ignore(responseStream.ignoreIf(), responseStream.ignoreCompletedIf())) {
                    responseStream.executor?.execute { responseStream.onCompleted() }
                        ?: responseStream.onCompleted()
                }
            }
        }
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
    val interceptors: List<ClientInterceptor> = listOf(),
    val retry: Retry = NoRetry
) {

    constructor(builder: Builder) : this(
        builder.credentials,
        builder.requestMetadata,
        builder.initialStreamRequests,
        builder.interceptors,
        builder.retry
    )

    @DecoratorMarker
    class Builder(
        internal var credentials: CallCredentials? = null,
        internal val requestMetadata: MutableMap<String, List<String>> = mutableMapOf(),
        internal val initialStreamRequests: MutableList<Any> = mutableListOf(),
        internal val interceptors: MutableList<ClientInterceptor> = mutableListOf(),
        internal var retry: Retry = NoRetry
    ) {

        constructor(opts: ClientCallOptions) : this(
            opts.credentials,
            opts.requestMetadata.toMutableMap(),
            opts.initialRequests.toMutableList(),
            opts.interceptors.toMutableList(),
            opts.retry
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

        /** Use the given [retry] settings */
        fun withRetry(retry: Retry) {
            this.retry = retry
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
    /** Called when the next result is available */
    var onNext: (RespT) -> Unit

    /** Called when the stream ends with an error. */
    var onError: (Throwable) -> Unit

    /** Called when the stream has ended an d will receive no more responses. */
    var onCompleted: () -> Unit

    /** Suppress [onNext], [onCompleted], and [onError] callbacks when the predicate is true. */
    var ignoreIf: () -> Boolean

    /** Suppress [onNext] callback when the predicate is true. */
    var ignoreNextIf: (RespT) -> Boolean

    /** Suppress [onError] callback when the predicate is true. */
    var ignoreErrorIf: (Throwable) -> Boolean

    /** Suppress [onCompleted] callback when the predicate is true. */
    var ignoreCompletedIf: () -> Boolean

    /** Executor to use for the result */
    var executor: Executor?
}

/** [ResponseStream] with defaults */
internal abstract class ResponseStreamImpl<RespT>(
    override var onNext: (RespT) -> Unit = {},
    override var onError: (Throwable) -> Unit = {},
    override var onCompleted: () -> Unit = {},
    override var ignoreIf: () -> Boolean = { false },
    override var ignoreNextIf: (RespT) -> Boolean = { _ -> false },
    override var ignoreErrorIf: (Throwable) -> Boolean = { _ -> false },
    override var ignoreCompletedIf: () -> Boolean = { false },
    override var executor: Executor? = null
) : ResponseStream<RespT>

/**
 * Result of a bi-directional streaming call including [requests] and [responses] streams.
 *
 * The [start] method must be called before accessing [requests] or [responses] and
 * should be called only once.
 */
interface StreamingCall<ReqT, RespT> {
    /** The request stream sent to the server */
    val requests: RequestStream<ReqT>
    /** The responses from the server */
    val responses: ResponseStream<RespT>

    /** Start the request and response streams */
    fun start(init: (ResponseStream<RespT>.() -> Unit) = {})
}

/**
 * Result of a client streaming call including the [requests] stream and a [response].
 *
 * The [start] method must be called before accessing [requests] or [response] and
 * should be called only once.
 */
interface ClientStreamingCall<ReqT, RespT> {
    /** The request stream sent to the server */
    val requests: RequestStream<ReqT>
    /** The response from the server */
    val response: ListenableFuture<RespT>

    /** Start the requests stream */
    fun start()
}

/**
 * Result of a server streaming call including the stream of [responses].
 *
 * The [start] method must be called before accessing [responses] and
 * should be called only once.
 */
interface ServerStreamingCall<RespT> {
    /** The responses from the server */
    val responses: ResponseStream<RespT>

    /** Start receiving responses */
    fun start(init: (ResponseStream<RespT>.() -> Unit) = {})
}

internal class StreamingCallImpl<ReqT, RespT>(
    private val begin: (StreamingCallImpl<ReqT, RespT>, RetryContext) -> Pair<RequestStream<ReqT>, ResponseStream<RespT>>
) : StreamingCall<ReqT, RespT> {

    private var init: (ResponseStream<RespT>.() -> Unit) = {}

    override lateinit var requests: RequestStream<ReqT>
    override lateinit var responses: ResponseStream<RespT>

    override fun start(init: (ResponseStream<RespT>.() -> Unit)) {
        this.init = init
        restart(RetryContext())
    }

    internal fun restart(retryContext: RetryContext) {
        val (req, resp) = begin(this, retryContext)
        requests = req
        responses = resp
        responses.apply(init)
    }
}

internal class ClientStreamingCallImpl<ReqT, RespT>(
    private val begin: (ClientStreamingCallImpl<ReqT, RespT>, RetryContext) -> RequestStream<ReqT>
) : ClientStreamingCall<ReqT, RespT> {

    override lateinit var requests: RequestStream<ReqT>
    override val response: SettableFuture<RespT> = SettableFuture.create()

    override fun start() {
        restart(RetryContext())
    }

    internal fun restart(retryContext: RetryContext) {
        requests = begin(this, retryContext)
    }
}

internal class ServerStreamingCallImpl<RespT>(
    private val begin: (ServerStreamingCallImpl<RespT>, RetryContext) -> ResponseStreamImpl<RespT>
) : ServerStreamingCall<RespT> {

    private var init: (ResponseStream<RespT>.() -> Unit) = {}

    override lateinit var responses: ResponseStream<RespT>

    override fun start(init: (ResponseStream<RespT>.() -> Unit)) {
        this.init = init
        restart(RetryContext())
    }

    internal fun restart(retryContext: RetryContext) {
        responses = begin(this, retryContext)
        responses.apply(init)
    }
}

/** Result of a server call with the response as a [ListenableFuture]. */
typealias FutureCall<T> = ListenableFuture<CallResult<T>>

@DecoratorMarker
class Callback<T> {
    /** Called when the call completes successfully. */
    var success: (CallResult<T>) -> Unit = {}

    /** Called when an error occurs. */
    var error: (Throwable) -> Unit = {}

    /** Suppress [success] and [error] callbacks when the predicate is true. */
    var ignoreIf: (() -> Boolean) = { false }

    /** Suppress [success] callback when the predicate is true. */
    var ignoreResultIf: ((CallResult<T>) -> Boolean) = { _ -> false }

    /** Suppress [error] callback when the predicate is true */
    var ignoreErrorIf: ((Throwable) -> Boolean) = { _ -> false }
}

/** Add a [callback] that will be run on the provided [executor] when the CallResult is available */
fun <T> FutureCall<T>.on(executor: Executor, callback: Callback<T>.() -> Unit) {
    Futures.addCallback<CallResult<T>>(this, object : FutureCallback<CallResult<T>> {
        val cb = Callback<T>().apply(callback)

        override fun onSuccess(result: CallResult<T>?) {
            if (!ignore(cb.ignoreIf(), cb.ignoreResultIf(result!!))) {
                cb.success(result)
            }
        }

        override fun onFailure(t: Throwable) {
            if (!ignore(cb.ignoreIf(), cb.ignoreErrorIf(t))) {
                cb.error(t)
            }
        }
    }, executor)
}

private fun ignore(vararg conditions: Boolean) = conditions.any { it }

/** Get the result of the future */
inline fun <T> ListenableFuture<T>.get(handler: (T) -> Unit) = handler(this.get())
