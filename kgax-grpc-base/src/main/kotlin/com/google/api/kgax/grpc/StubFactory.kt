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

import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.GoogleCredentials
import com.google.common.util.concurrent.ListenableFuture
import io.grpc.CallCredentials
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.auth.MoreCallCredentials
import io.grpc.stub.AbstractStub
import io.grpc.stub.StreamObserver
import java.io.InputStream
import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass

/**
 * Factory for gRPC stubs to simplify their construction.
 *
 * If a [Channel] is not provided one will be created automatically and you must
 * call [ManagedChannel.shutdown] when you are done using it (i.e. often when your
 * application is about to exit, sleep, etc.)
 *
 * The same channel is used by all stubs created by this factory.
 */
class StubFactory<T : AbstractStub<T>> {
    private val stubType: KClass<T>
    val channel: ManagedChannel

    /**
     * Creates a new factory using an the available gRPC implementation for the provided [stubType]
     * that will communicate with the server at the given [host] and [port].
     *
     * If [enableRetry] is enabled then failed operations will be retried when it's safe to do so.
     *
     * This method will create a new channel via the [channel] property. Don't forget to call
     * [ManagedChannel.shutdown] to dispose of the channel when it is no longer needed.
     */
    constructor(stubType: KClass<T>, host: String = "localhost", port: Int = 8080, enableRetry: Boolean = false) :
        this(stubType, ManagedChannelBuilder.forAddress(host, port), {
            if (enableRetry) {
                enableRetry()
            }
        })

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
     * Creates a new factory with the given [builder].
     *
     * Don't forget to call [ManagedChannel.shutdown] to dispose of the channel when it is no
     * longer needed.
     */
    constructor(
        stubType: KClass<T>,
        builder: ManagedChannelBuilder<*>,
        init: ManagedChannelBuilder<*>.() -> Unit = {}
    ) {
        this.stubType = stubType
        builder.apply(init)
        this.channel = builder.build()
    }

    /**
     * Create a new stub.
     */
    fun newStub() = fromCallCredentials()

    /**
     * Creates a stub from a service account JSON [keyFile] with the provided [oauthScopes].
     */
    fun fromServiceAccount(keyFile: InputStream, oauthScopes: List<String>) =
        fromCallCredentials(
            MoreCallCredentials.from(
                GoogleCredentials.fromStream(keyFile).createScoped(oauthScopes)
            )
        )

    /**
     * Creates a stub from a access [token] with the provided [oauthScopes].
     */
    fun fromAccessToken(token: AccessToken, oauthScopes: List<String>) =
        fromCallCredentials(
            MoreCallCredentials.from(
                GoogleCredentials.create(token).createScoped(oauthScopes)
            )
        )

    internal fun fromCallCredentials(credentials: CallCredentials? = null): GrpcClientStub<T> {
        // instantiate stub
        try {
            val constructor = stubType.java
                .declaringClass
                .getMethod(getFactoryMethodName(stubType.java), Channel::class.java)

            @Suppress("UNCHECKED_CAST")
            return GrpcClientStub(
                constructor.invoke(null, channel) as T,
                ClientCallOptions(credentials = credentials)
            )
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
                it.returnType.name == ListenableFuture::class.java.name
            } -> "newFutureStub"
            type.methods.any {
                it.returnType.name == StreamObserver::class.java.name
            } -> "newStub"
            type.methods.flatMap { it.parameterTypes.asIterable() }.any {
                it.name == StreamObserver::class.java.name
            } -> "newStub"
            else -> "newBlockingStub"
        }
    }
}
