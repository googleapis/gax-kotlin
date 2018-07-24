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

package com.google.kgax.examples.grpc

import android.os.AsyncTask
import android.os.Bundle
import android.support.v7.app.AppCompatActivity
import android.widget.TextView
import com.google.cloud.language.v1.AnalyzeEntitiesRequest
import com.google.cloud.language.v1.AnalyzeEntitiesResponse
import com.google.cloud.language.v1.Document
import com.google.cloud.language.v1.LanguageServiceGrpc
import com.google.kgax.grpc.GrpcClientStub
import com.google.kgax.grpc.StubFactory

/**
 * Kotlin example using KGax with gRPC.
 *
 * This is the same as [MainActivity], but shows the use of a future based stub
 * rather than a blocking stub.
 *
 * @author jbolinger
 */
class FutureActivity : AppCompatActivity() {
    private val factory = StubFactory(
            LanguageServiceGrpc.LanguageServiceFutureStub::class,
            "language.googleapis.com")

    private val stub by lazy {
        applicationContext.resources.openRawResource(R.raw.sa).use {
            factory.fromServiceAccount(it,
                    listOf("https://www.googleapis.com/auth/cloud-platform"))
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        val resultText: TextView = findViewById(R.id.result_text)

        // call the api
        ApiTestTask(stub) { resultText.text = it }.execute()
    }

    override fun onDestroy() {
        super.onDestroy()

        // clean up
        factory.shutdown()
    }

    private class ApiTestTask(
            val stub: GrpcClientStub<LanguageServiceGrpc.LanguageServiceFutureStub>,
            val onResult: (String) -> Unit
    ) : AsyncTask<Unit, Unit, AnalyzeEntitiesResponse>() {
        override fun doInBackground(vararg params: Unit) : AnalyzeEntitiesResponse {
            val response = stub.executeFuture { it ->
                it.analyzeEntities(AnalyzeEntitiesRequest.newBuilder()
                        .setDocument(Document.newBuilder()
                                .setContent("Hi there Joe")
                                .setType(Document.Type.PLAIN_TEXT)
                                .build())
                        .build())
            }
            val (body, metadata) = response.get()
            return body
        }

        override fun onPostExecute(result: AnalyzeEntitiesResponse) {
             onResult("The API says: $result")
        }
    }
}
