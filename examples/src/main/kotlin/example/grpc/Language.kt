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

package example.grpc

import com.google.api.kgax.grpc.StubFactory
import com.google.cloud.language.v1.AnalyzeEntitiesRequest
import com.google.cloud.language.v1.Document
import com.google.cloud.language.v1.LanguageServiceGrpc
import kotlinx.coroutines.runBlocking
import java.io.File

/**
 * Simple example of calling the Language API with KGax.
 *
 * Run this example using your service account as follows:
 *
 * ```
 * $ CREDENTIALS=<path_to_your_service_account.json> ./gradlew examples:run --args language
 * ```
 */
fun languageExample(credentials: String) = runBlocking {
    // create a stub factory
    val factory = StubFactory(
        LanguageServiceGrpc.LanguageServiceFutureStub::class,
        "language.googleapis.com", 443
    )

    // create a stub
    val stub = File(credentials).inputStream().use {
        factory.fromServiceAccount(it, listOf("https://www.googleapis.com/auth/cloud-platform"))
    }

    // call the API
    val response = stub.execute {
        it.analyzeEntities(
            AnalyzeEntitiesRequest.newBuilder().apply {
                document = Document.newBuilder().apply {
                    content = "Hi there Joe"
                    type = Document.Type.PLAIN_TEXT
                }.build()
            }.build()
        )
    }

    println("The API says: ${response.body}")

    // shutdown all connections
    factory.shutdown()
}
