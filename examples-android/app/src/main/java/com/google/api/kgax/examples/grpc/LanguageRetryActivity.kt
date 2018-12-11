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

package com.google.api.kgax.examples.grpc

import android.os.Bundle
import android.support.test.espresso.idling.CountingIdlingResource
import android.util.Log
import com.google.api.kgax.Retry
import com.google.api.kgax.RetryContext
import com.google.api.kgax.grpc.StubFactory
import com.google.cloud.language.v1.AnalyzeEntitiesRequest
import com.google.cloud.language.v1.Document
import com.google.cloud.language.v1.LanguageServiceGrpc
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Kotlin example showcasing client side retries using KGax with gRPC and the
 * Google Natural Language API.
 */
class LanguageRetryActivity : AbstractExampleActivity<LanguageServiceGrpc.LanguageServiceFutureStub>(
    CountingIdlingResource("LanguageRetry")
) {
    override val factory = StubFactory(
        LanguageServiceGrpc.LanguageServiceFutureStub::class,
        "language.googleapis.com"
    )

    override val stub by lazy {
        applicationContext.resources.openRawResource(R.raw.sa).use {
            factory.fromServiceAccount(
                it,
                listOf("https://www.googleapis.com/auth/cloud-platform")
            )
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        // call the api
        launch(Dispatchers.Main) {
            val response = stub.prepare {
                withRetry(RetryForever)
            }.execute { it ->
                it.analyzeEntities(
                    AnalyzeEntitiesRequest.newBuilder().apply {
                        document = Document.newBuilder().apply {
                            content = "Hi there Joe"
                            type = Document.Type.PLAIN_TEXT
                        }.build()
                    }.build()
                )
            }

            updateUIWithExampleResult(response.body.toString())
        }
    }
}

object RetryForever : Retry {
    override fun retryAfter(error: Throwable, context: RetryContext): Long? {
        Log.i("Retry", "Will attempt retry #${context.numberOfAttempts}")

        // retry after a 500 ms delay
        return 500
    }
}
