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
import com.google.api.kgax.grpc.StubFactory
import com.google.api.kgax.grpc.executeLongRunning
import com.google.cloud.speech.v1.LongRunningRecognizeRequest
import com.google.cloud.speech.v1.LongRunningRecognizeResponse
import com.google.cloud.speech.v1.RecognitionAudio
import com.google.cloud.speech.v1.RecognitionConfig
import com.google.cloud.speech.v1.SpeechGrpc
import com.google.common.io.ByteStreams
import com.google.protobuf.ByteString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

private const val TAG = "APITest"

/**
 * Kotlin example showcasing long running operations using KGax with gRPC and the Google Speech API.
 */
class SpeechActivity : AbstractExampleActivity<SpeechGrpc.SpeechFutureStub>(
    CountingIdlingResource("Speech")
) {

    override val factory = StubFactory(
        SpeechGrpc.SpeechFutureStub::class, "speech.googleapis.com"
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

        // get audio
        val audioData = applicationContext.resources.openRawResource(R.raw.audio).use {
            ByteString.copyFrom(ByteStreams.toByteArray(it))
        }

        // call the api
        launch(Dispatchers.Main) {
            // execute a long running operation
            val lro = stub.executeLongRunning(LongRunningRecognizeResponse::class.java) {
                it.longRunningRecognize(
                    LongRunningRecognizeRequest.newBuilder().apply {
                        audio = RecognitionAudio.newBuilder().apply {
                            content = audioData
                        }.build()
                        config = RecognitionConfig.newBuilder().apply {
                            languageCode = "en-US"
                            encoding = RecognitionConfig.AudioEncoding.LINEAR16
                            sampleRateHertz = 16000
                        }.build()
                    }.build()
                )
            }

            // wait for the response to complete
            Log.i(TAG, "Waiting for long running operation...")
            val (response, _) = lro.await()

            Log.i(TAG, "Operation completed: ${lro.operation?.name}")
            updateUIWithExampleResult(response.toString())
        }
    }
}
