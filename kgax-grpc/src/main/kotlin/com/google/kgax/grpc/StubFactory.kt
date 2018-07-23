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
import com.google.common.util.concurrent.ListenableFuture
import io.grpc.CallCredentials
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.auth.MoreCallCredentials
import io.grpc.okhttp.OkHttpChannelBuilder
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import io.grpc.stub.StreamObserver
import java.io.InputStream
import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass

inline fun <T> ListenableFuture<T>.get(handler: (T) -> Unit) = handler(this.get())

/**
 * Factory for gRPC stubs to simplify their construction.
 *
 * If a [Channel] is not provided one will be created automatically and you must
 * call [ManagedChannel.shutdown] when you are done using it (i.e. often when your
 * application is about to exit, sleep, etc.)
 */
class StubFactory<T : AbstractStub<T>> {
    private val stubType: KClass<T>
    val channel: ManagedChannel

    /**
     * Creates a new factory for the [stubType] with the given [channel].
     *
     * Don't forget to call [ManagedChannel.shutdown] to dispose of the channel when it is no
     * longer needed.
     */
    constructor (stubType: KClass<T>, channel: ManagedChannel) {
        this.stubType = stubType
        this.channel = channel
    }

    /**
     * Creates a new factory using an [OkHttpChannelBuilder] for the provided [stubType]
     * that will communicate with the server at the given [host] and [port].
     *
     * If [enableRetry] is enabled then failed operations will be retried when it's safe to do so.
     *
     * This method will create a new channel via the [channel] property. Don't forget to call
     * [ManagedChannel.shutdown] to dispose of the channel when it is no longer needed.
     */
    constructor (stubType: KClass<T>, host: String, port: Int = 443, enableRetry: Boolean = true) {
        // construct channel
        val channelBuilder = OkHttpChannelBuilder.forAddress(host, port)
        if (enableRetry) {
            channelBuilder.enableRetry()
        }

        this.stubType = stubType
        this.channel = channelBuilder.build()
    }

    /**
     * Creates a stub from a service account JSON [keyFile] with the provided [oauthScopes]
     * and any additional [options].
     */
    @JvmOverloads
    fun fromServiceAccount(
        keyFile: InputStream,
        oauthScopes: List<String>,
        options: ClientOptions = ClientOptions()
    ) =
            fromCallCredentials(MoreCallCredentials.from(
                    GoogleCredentials.fromStream(keyFile).createScoped(oauthScopes)), options)

    /**
     * Creates a stub from a access [token] with the provided [oauthScopes] and any additional
     * [options].
     */
    @JvmOverloads
    fun fromAccessToken(
        token: AccessToken,
        oauthScopes: List<String>,
        options: ClientOptions = ClientOptions()
    ) =
            fromCallCredentials(MoreCallCredentials.from(
                    GoogleCredentials.create(token).createScoped(oauthScopes)), options)

    internal fun fromCallCredentials(
        credentials: CallCredentials,
        options: ClientOptions = ClientOptions()
    ): T {
        // instantiate stub
        try {
            val constructor = stubType.java
                    .declaringClass
                    .getMethod(getFactoryMethodName(stubType.java), Channel::class.java)
            return (constructor.invoke(null, channel) as T)
                    .withCallCredentials(credentials)
                    .decorate(options)
        } catch (e: NoSuchMethodException) {
            throw IllegalArgumentException("Invalid stub type (missing static factory method)", e)
        } catch (ex: Exception) {
            throw IllegalArgumentException("Unable to create stub", ex)
        }
    }

    /**
     * Shuts down the channel associated with the factories stubs.
     *
     * This is a convenience method. You are free to shutdown the channel in other
     * ways without calling this method.
     */
    @JvmOverloads
    fun shutdown(waitForSeconds: Long = 5) {
        channel.shutdown().awaitTermination(waitForSeconds, TimeUnit.SECONDS)
    }

    private fun getFactoryMethodName(type: Class<T>): String {
        return when {
            type.methods.any {
                it.returnType.name.equals(ListenableFuture::class.java.name)
            } -> "newFutureStub"
            type.methods.any {
                it.returnType.name.equals(StreamObserver::class.java.name)
            } -> "newStub"
            type.methods.flatMap { it.parameterTypes.asIterable() }.any {
                it.name.equals(StreamObserver::class.java.name)
            } -> "newStub"
            else -> "newBlockingStub"
        }
    }
}

/**
 * Apply the extra [options] to decorate the stub.
 *
 * The default value is recommended for most applications, which includes support for:
 *   + [ResponseMetadata]
 */
fun <T : AbstractStub<T>> T.decorate(options: ClientOptions = ClientOptions()): T {
    var ret = this
    if (options.enableResponseMetadata) {
        ret = ret.withInterceptors(ResponseMetadataInterceptor())
    }
    return ret
}

/** Apply the extra [options] to the stub and configure authentication with the given [credentials]. */
fun <T : AbstractStub<T>> T.decorate(credentials: GoogleCredentials, options: ClientOptions = ClientOptions()): T =
        this.withCallCredentials(MoreCallCredentials.from(credentials)).decorate(options)

/**
 * Prepare a decorated call to create a [ClientCall]. For example:
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
 * Use this method with the appropriate [ClientCall] method, such as [ClientCall.executeBlocking]
 * instead of calling methods on the gRPC stubs directly when you want to use the additional
 * functionality provided by this library.
 */
fun <T : AbstractStub<T>> T.prepare(init: ClientCallOptions.Builder.() -> Unit = {}): ClientCall<T> {
    val builder = ClientCallOptions.Builder()
    builder.init()
    return this.prepare(ClientCallOptions(builder))
}

/** Prepare a decorated call */
fun <T : AbstractStub<T>> T.prepare(options: ClientCallOptions): ClientCall<T> {
    val opts = ClientCallOptions(options)
    opts.responseMetadata = ResponseMetadata()

    // prepare call
    var stub = this.withOption(ResponseMetadata.KEY, opts.responseMetadata)

    // add request metadata
    if (!opts.requestMetadata.isEmpty()) {
        val header = io.grpc.Metadata()
        for ((k, v) in opts.requestMetadata) {
            val key = io.grpc.Metadata.Key.of(k, io.grpc.Metadata.ASCII_STRING_MARSHALLER)
            v.forEach { header.put(key, it) }
        }
        stub = MetadataUtils.attachHeaders(stub, header)
    }

    // add auth
    if (opts.credentials != null) {
        stub = stub.withCallCredentials(opts.credentials)
    }

    // add advanced features
    if (opts.interceptors.any()) {
        stub = stub.withInterceptors(*opts.interceptors.toTypedArray())
    }

    // save the options
    return ClientCall(stub, opts)
}
