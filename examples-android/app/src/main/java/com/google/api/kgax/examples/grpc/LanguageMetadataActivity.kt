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
import com.google.api.kgax.grpc.StubFactory
import com.google.cloud.language.v1.AnalyzeEntitiesRequest
import com.google.cloud.language.v1.Document
import com.google.cloud.language.v1.LanguageServiceGrpc
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext

/**
 * Kotlin example showcasing request & response metadata using KGax with gRPC and the
 * Google Natural Language API.
 */
class LanguageMetadataActivity : AbstractExampleActivity<LanguageServiceGrpc.LanguageServiceFutureStub>(
    CountingIdlingResource("LanguageMetadata")
) {
    lateinit var job: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Main + job

    override val factory = StubFactory(
        LanguageServiceGrpc.LanguageServiceFutureStub::class,
        "language.googleapis.com", 443
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

        job = Job()

        // call the api
        launch(Dispatchers.Main) {
            val (_, metadata) = stub.prepare {
                withMetadata("foo", listOf("1", "2"))
                withMetadata("bar", listOf("a", "b"))
            }.execute {
                it.analyzeEntities(
                    AnalyzeEntitiesRequest.newBuilder().apply {
                        document = Document.newBuilder().apply {
                            content = "Hi there Joe"
                            type = Document.Type.PLAIN_TEXT
                        }.build()
                    }.build()
                )
            }

            // stringify the metadata
            val text = metadata.keys().joinToString("\n") { key ->
                val value = metadata.getAll(key)?.joinToString(", ")
                "$key=[$value]"
            }
            updateUIWithExampleResult(text)
        }
    }
}
