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
import android.util.Log
import android.widget.TextView
import com.google.cloud.language.v1.AnalyzeEntitiesRequest
import com.google.cloud.language.v1.Document
import com.google.cloud.language.v1.LanguageServiceGrpc
import com.google.kgax.grpc.StubFactory
import com.google.kgax.grpc.prepare

private const val TAG = "APITest"

/**
 * Kotlin example showcasing request & response metadata using KGax with gRPC.
 *
 * @author jbolinger
 */
class MetadataActivity : AppCompatActivity() {

    private val factory = StubFactory(
            LanguageServiceGrpc.LanguageServiceBlockingStub::class,
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
            val stub: LanguageServiceGrpc.LanguageServiceBlockingStub,
            val onResult: (String) -> Unit
    ) : AsyncTask<Unit, Unit, String>() {
        override fun doInBackground(vararg params: Unit): String {
            val (response, metadata) = stub.prepare {
                withMetadata("foo", listOf("1", "2"))
                withMetadata("bar", listOf("a", "b"))
            }.executeBlocking {
                it.analyzeEntities(AnalyzeEntitiesRequest.newBuilder()
                        .setDocument(Document.newBuilder()
                                .setContent("Hi there Joe")
                                .setType(Document.Type.PLAIN_TEXT)
                                .build())
                        .build())
            }

            // stringify the metadata
            return metadata.keys().joinToString("\n") { key ->
                val value = metadata.getAll(key)?.joinToString(", ")
                "$key=[$value]"
            }
        }

        override fun onPostExecute(metadata: String) {
            onResult("The API responded with metadata keys of: $metadata")
        }
    }
}
