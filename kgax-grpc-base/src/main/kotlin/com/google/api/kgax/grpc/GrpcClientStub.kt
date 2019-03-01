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

import com.google.api.kgax.NoRetry
import com.google.api.kgax.Page
import com.google.api.kgax.Retry
import com.google.api.kgax.RetryContext
import com.google.api.kgax.createPager
import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.GoogleCredentials
import com.google.common.util.concurrent.ListenableFuture
import com.google.protobuf.MessageLite
import io.grpc.CallCredentials
import io.grpc.ClientInterceptor
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.auth.MoreCallCredentials
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.guava.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.InputStream

@DslMarker
annotation class DecoratorMarker

/**
 * A convenience wrapper for gRPC stubs that enables ease of use and the
 * addition of some non-native functionality.
 *
 * You don't typically need to create an instance of this class directly. Instead use
 * the [prepare] method that this library defines for all gRPC stubs, which will
 * ensure the [originalStub] and [options] are setup correctly.
 *
 * For example:
 *
 * ```
 * val stub = StubFactory(MyBlockingStub::class, "host.example.com")
 *                .fromServiceAccount(keyFile, listOf("https://host.example.com/auth/my-scope"))
 * val response = stub.execute { it -> it.someApiMethod(...) }
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
     * Use this method with the appropriate [GrpcClientStub] method, such as [execute]
     * instead of calling methods on the gRPC stubs directly when you want to use the additional
     * functionality provided by this library.
     */
    fun prepare(init: ClientCallOptions.Builder.() -> Unit = {}): GrpcClientStub<T> {
        val builder = ClientCallOptions.Builder(options)
        builder.init()
        return GrpcClientStub(
            originalStub,
            ClientCallOptions(builder)
        )
    }

    /**
     * Execute a future-based gRPC call via a coroutine. For example:
     *
     * ```
     * val response = stub.prepare {
     *     withMetadata("foo", listOf("bar"))
     * }.execute {
     *     it.myFutureMethod(...)
     * }
     * print("${response.body}")
     * ```
     *
     * The [method] lambda should perform a future method call on the stub given as the
     * first parameter. The result along with any additional information, such as
     * [ResponseMetadata], will be returned.
     *
     * An optional [context] can be supplied to enable arbitrary retry strategies.
     */
    suspend fun <RespT : MessageLite> execute(
        context: String = "",
        method: (T) -> ListenableFuture<RespT>
    ): CallResult<RespT> {
        var retryContext = RetryContext(context)

        while (true) {
            try {
                val stub = stubWithContext()
                val result = method(stub).await()
                return CallResult(result, stub.context.responseMetadata)
            } catch (cancel: CancellationException) {
                throw cancel
            } catch (throwable: Throwable) {
                val retryAfter = options.retry.retryAfter(throwable, retryContext)
                if (retryAfter != null) {
                    retryContext = retryContext.next()
                    delay(retryAfter)
                } else {
                    throw throwable
                }
            }
        }
    }

    /**
     * Execute a bidirectional streaming call. For example:
     *
     * ```
     * val streams = stub.prepare().executeStreaming { it::myStreamingMethod }
     *
     * // send outbound requests
     * streams.requests.send(...)
     * streams.requests.send(...)
     *
     * // process incoming responses
     * for (response in streams.responses) {
     *     println("response: $response")
     * }
     * ```
     *
     * The [method] lambda should return a bound method reference on the stub that is provided
     * as the first parameter to the lambda, as shown in the example. There is no need to call
     * the stub method directly. It will be done as part of this call. The result of this method
     * will provide a pair of inbound and outbound streams.
     *
     * An optional [context] can be supplied to enable arbitrary retry strategies.
     */
    @ExperimentalCoroutinesApi
    suspend fun <ReqT : MessageLite, RespT : MessageLite> executeStreaming(
        context: String = "",
        method: (T) -> (StreamObserver<RespT>) -> StreamObserver<ReqT>
    ): StreamingCall<ReqT, RespT> = coroutineScope {
        val requestChannel = Channel<ReqT>(Channel.UNLIMITED)
        val responseChannel = Channel<RespT>(Channel.UNLIMITED)

        val invoke = { stub: T, responseStream: StreamObserver<RespT> -> method(stub)(responseStream) }
        executeStreaming(context, invoke, requestChannel = requestChannel, responseChannel = responseChannel)

        StreamingCall(requestChannel, responseChannel)
    }

    /**
     * Execute a call that sends a stream of requests to the server. For example:
     *
     * ```
     * val streams = stub.prepare().executeClientStreaming { it::myStreamingMethod }
     *
     * // send outbound requests
     * streams.requests.send(...)
     * streams.requests.send(...)
     *
     * // process the response when available
     * print("response: ${streams.response.await()}") }
     * ```
     *
     * The [method] lambda should return a bound method reference on the stub that is provided
     * as the first parameter to the lambda, as shown in the example. There is no need to call
     * the stub method directly. It will be done as part of this call. The result of this method
     * will provide an outbound stream for requests to be sent to the server and a future for
     * the server's response.
     *
     * An optional [context] can be supplied to enable arbitrary retry strategies.
     */
    @ExperimentalCoroutinesApi
    suspend fun <ReqT : MessageLite, RespT : MessageLite> executeClientStreaming(
        context: String = "",
        method: (T) -> (StreamObserver<RespT>) -> StreamObserver<ReqT>
    ): ClientStreamingCall<ReqT, RespT> = coroutineScope {
        val requestChannel = Channel<ReqT>(Channel.UNLIMITED)
        val response = CompletableDeferred<RespT>()

        val invoke = { stub: T, responseStream: StreamObserver<RespT> -> method(stub)(responseStream) }
        executeStreaming(context, invoke, requestChannel = requestChannel, response = response)

        ClientStreamingCall(requestChannel, response)
    }

    /**
     * Execute a call that receives one-way streaming responses from the server. For example:
     *
     * ```
     * val request = MyRequest(...)
     * val streams = stub.prepare().executeServerStreaming { it, observer ->
     *     it.myStreamingMethod(request, observer)
     * }
     *
     * // process incoming responses
     * for (response in streams.responses) {
     *     println("response: $response")
     * }
     * ```
     *
     * The [method] lambda should call a streaming method reference on the stub that is provided
     * as the first parameter to the lambda using the observer given as the second parameter.
     * The result of this method will provide the inbound stream of responses from the server.
     *
     * An optional [context] can be supplied to enable arbitrary retry strategies.
     */
    @ExperimentalCoroutinesApi
    suspend fun <RespT : MessageLite> executeServerStreaming(
        context: String = "",
        method: (T, StreamObserver<RespT>) -> Unit
    ): ServerStreamingCall<RespT> = coroutineScope {
        val responseChannel = Channel<RespT>(Channel.UNLIMITED)

        val invoke: (T, StreamObserver<RespT>) -> StreamObserver<RespT>? = { stub, responseStream ->
            method(stub, responseStream)
            null
        }
        executeStreaming(context, invoke, responseChannel = responseChannel)

        ServerStreamingCall(responseChannel)
    }

    @ExperimentalCoroutinesApi
    private suspend fun <ReqT : MessageLite, RespT : MessageLite> executeStreaming(
        context: String,
        method: (T, StreamObserver<RespT>) -> StreamObserver<ReqT>?,
        requestChannel: Channel<ReqT>? = null,
        responseChannel: Channel<RespT>? = null,
        response: CompletableDeferred<RespT>? = null
    ) = coroutineScope {
        var canRetry = true
        var completed = false
        var cancelled = false

        // these change during a retry
        lateinit var stub: T
        var requestObserver: StreamObserver<ReqT>? = null

        // pipe all requests to the observer
        val requestWriter = requestChannel?.let { channel ->
            GlobalScope.launch {
                for (next in channel) {
                    requestObserver?.onNext(next)
                }
            }
        }

        // start function
        fun start(retryContext: RetryContext) {
            stub = stubWithContext()

            // invoke method
            requestObserver = method(stub, object : StreamObserver<RespT> {
                override fun onNext(value: RespT) {
                    canRetry = false

                    responseChannel?.let { runBlocking { it.send(value) } }
                    response?.complete(value)
                }

                override fun onError(t: Throwable) {
                    val retryAfter = if (canRetry) options.retry.retryAfter(t, retryContext) else null
                    if (retryAfter != null) {
                        runBlocking {
                            delay(retryAfter)
                            start(retryContext.next())
                        }
                        return
                    }

                    completed = true
                    cancelled = when (t) {
                        is StatusRuntimeException -> t.status.code == Status.Code.CANCELLED
                        else -> false
                    }

                    if (cancelled) {
                        responseChannel?.close()
                    } else {
                        responseChannel?.close(t)
                    }
                    response?.completeExceptionally(t)
                    requestChannel?.close()
                    runBlocking { requestWriter?.join() }
                }

                override fun onCompleted() {
                    canRetry = false
                    completed = true

                    responseChannel?.close()
                    requestChannel?.close()
                    runBlocking { requestWriter?.join() }
                }
            })

            // add and initial requests
            if (requestChannel != null) {
                for (request in options.initialRequests) {
                    runBlocking {
                        @Suppress("UNCHECKED_CAST")
                        requestChannel.send(request as ReqT)
                    }
                }
            }
        }

        // start now
        start(RetryContext(context))

        // add shutdown handlers
        responseChannel?.invokeOnClose { error ->
            if (!completed && error == null) {
                stub.context.call.cancel(
                    "explicit close() called by client",
                    CancellationException()
                )
            }
        }
        requestChannel?.invokeOnClose {
            runBlocking { requestWriter?.join() }
            if (!cancelled) {
                requestObserver?.onCompleted()
            }
        }
    }

    /**
     * Gets a one-time use stub with an initial (empty) context.
     *
     * This method is used internally before each API method call, and is only
     * useful for creating additional helper methods like [execute].
     */
    fun stubWithContext(): T {
        // add gax interceptor
        var stub = originalStub
            .withInterceptors(GAXInterceptor())
            .withOption(
                ClientCallContext.KEY,
                ClientCallContext()
            )

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
fun <T : AbstractStub<T>> T.prepare(options: ClientCallOptions) =
    GrpcClientStub(this, options)

/**
 * Decorated call options. The settings apply on a per-call level and can be created
 * using the clientCallOptions builder method:
 *
 * val opts = clientCallOptions {
 *     withAccessToken(...)
 *     withMetadata(...)
 * }
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

fun clientCallOptions(init: ClientCallOptions.Builder.() -> Unit = {}): ClientCallOptions {
    val builder = ClientCallOptions.Builder()
    builder.apply(init)
    return builder.build()
}

/** Result of the call with the response [body] associated [metadata]. */
data class CallResult<RespT>(val body: RespT, val metadata: ResponseMetadata) {

    /** Map the body of this result to another type using the [transform]. */
    fun <R> map(transform: (RespT) -> R) = CallResult(transform(this.body), this.metadata)
}

/**
 * Result of a bi-directional streaming call including [requests] and [responses] streams.
 */
class StreamingCall<ReqT, RespT>(
    val requests: SendChannel<ReqT>,
    val responses: ReceiveChannel<RespT>
)

/**
 * Result of a client streaming call including the [requests] stream and a deferred [response].
 */
class ClientStreamingCall<ReqT, RespT>(
    val requests: SendChannel<ReqT>,
    val response: Deferred<RespT>
)

/**
 * Result of a server streaming call including the stream of [responses].
 */
class ServerStreamingCall<RespT>(
    val responses: ReceiveChannel<RespT>
)

/** Result of a call with paging */
data class PageWithMetadata<T>(
    override val elements: Iterable<T>,
    override val token: String,
    val metadata: ResponseMetadata
) : Page<T, String>

/**
 * Create a stream of [Page]s.
 *
 * ```
 * val pager = pager<ListLogEntriesRequest, ListLogEntriesResponse, LogEntry> {
 *      method = stub::listLogEntries
 *      initialRequest = {
 *          ListLogEntriesRequest.newBuilder()
 *                  .addResourceNames(project)
 *                  .setFilter("logName=$log")
 *                  .setPageSize(10)
 *                  .build()
 *      }
 *      nextRequest = { request, token ->
 *          request.toBuilder().setPageToken(token).build()
 *      }
 *      nextPage = { response ->
 *          PageWithMetadata(response.entriesList, response.nextPageToken)
 *      }
 *  }
 *
 *  // go through all pages
 *  for (page in pager) {
 *      for (entry in page.elements) {
 *          println(entry.textPayload)
 *      }
 *  }
 * ```
 */
@ExperimentalCoroutinesApi
suspend fun <ReqT, RespT, ElementT> pager(
    method: suspend (ReqT) -> RespT,
    initialRequest: () -> ReqT,
    nextRequest: (ReqT, String) -> ReqT,
    nextPage: (RespT) -> PageWithMetadata<ElementT>
): ReceiveChannel<PageWithMetadata<ElementT>> = createPager(
    method = method,
    initialRequest = initialRequest,
    nextRequest = nextRequest,
    nextPage = nextPage,
    hasNextPage = { p -> p.elements.any() && p.token.isNotEmpty() }
)
