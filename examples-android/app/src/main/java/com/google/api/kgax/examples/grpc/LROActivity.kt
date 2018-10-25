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

import android.os.AsyncTask
import android.os.Bundle
import android.support.v7.app.AppCompatActivity
import android.util.Log
import android.widget.TextView
import com.google.cloud.speech.v1.LongRunningRecognizeRequest
import com.google.cloud.speech.v1.LongRunningRecognizeResponse
import com.google.cloud.speech.v1.RecognitionAudio
import com.google.cloud.speech.v1.RecognitionConfig
import com.google.cloud.speech.v1.SpeechGrpc
import com.google.common.io.ByteStreams
import com.google.api.kgax.grpc.GrpcClientStub
import com.google.api.kgax.grpc.StubFactory
import com.google.protobuf.ByteString

private const val TAG = "APITest"

/**
 * Kotlin example showcasing LRO using KGax with gRPC.
 */
class LROActivity : AppCompatActivity() {

    private val factory = StubFactory(
        SpeechGrpc.SpeechFutureStub::class, "speech.googleapis.com"
    )

    private val stub by lazy {
        applicationContext.resources.openRawResource(R.raw.sa).use {
            factory.fromServiceAccount(
                it,
                listOf("https://www.googleapis.com/auth/cloud-platform")
            )
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        val resultText: TextView = findViewById(R.id.result_text)

        // get audio
        val audio = applicationContext.resources.openRawResource(R.raw.audio).use {
            ByteString.copyFrom(ByteStreams.toByteArray(it))
        }

        // call the api
        ApiTestTask(stub, audio) { resultText.text = it }.execute()
    }

    override fun onDestroy() {
        super.onDestroy()

        // clean up
        factory.shutdown()
    }

    private class ApiTestTask(
        val stub: GrpcClientStub<SpeechGrpc.SpeechFutureStub>,
        val audio: ByteString,
        val onResult: (String) -> Unit
    ) : AsyncTask<Unit, Unit, LongRunningRecognizeResponse>() {
        override fun doInBackground(vararg params: Unit): LongRunningRecognizeResponse {
            // execute a long running operation
            val lro = stub
                .executeLongRunning(LongRunningRecognizeResponse::class.java) {
                    it.longRunningRecognize(
                        LongRunningRecognizeRequest.newBuilder()
                            .setAudio(
                                RecognitionAudio.newBuilder()
                                    .setContent(audio)
                                    .build()
                            )
                            .setConfig(
                                RecognitionConfig.newBuilder()
                                    .setEncoding(RecognitionConfig.AudioEncoding.LINEAR16)
                                    .setSampleRateHertz(16000)
                                    .setLanguageCode("en-US")
                                    .build()
                            )
                            .build()
                    )
                }

            // wait for the response to complete
            Log.i(TAG, "Waiting for long running operation...")
            val (response, _) = lro.get()

            Log.i(TAG, "Operation completed: ${lro.operation?.name}")
            return response
        }

        override fun onPostExecute(result: LongRunningRecognizeResponse?) {
            onResult("The API says: $result")
        }
    }
}
