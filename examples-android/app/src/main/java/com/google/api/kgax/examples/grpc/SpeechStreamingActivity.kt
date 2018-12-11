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

import android.Manifest
import android.content.pm.PackageManager
import android.os.Bundle
import android.support.test.espresso.idling.CountingIdlingResource
import android.support.v4.app.ActivityCompat
import android.util.Log
import com.google.api.kgax.examples.grpc.util.AudioEmitter
import com.google.api.kgax.grpc.StubFactory
import com.google.cloud.speech.v1.RecognitionConfig
import com.google.cloud.speech.v1.SpeechGrpc
import com.google.cloud.speech.v1.StreamingRecognitionConfig
import com.google.cloud.speech.v1.StreamingRecognizeRequest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

private const val TAG = "APITest"
private const val REQUEST_RECORD_AUDIO_PERMISSION = 200
private val PERMISSIONS = arrayOf(Manifest.permission.RECORD_AUDIO)

/**
 * Kotlin example showcasing streaming APIs using KGax with gRPC and the Google Speech API.
 */
@ExperimentalCoroutinesApi
class SpeechStreamingActivity : AbstractExampleActivity<SpeechGrpc.SpeechStub>(
    CountingIdlingResource("SpeechStreaming")
) {
    private var permissionToRecord = false
    private val audioEmitter: AudioEmitter = AudioEmitter()

    override val factory = StubFactory(
        SpeechGrpc.SpeechStub::class, "speech.googleapis.com"
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

        // get permissions
        ActivityCompat.requestPermissions(this, PERMISSIONS, REQUEST_RECORD_AUDIO_PERMISSION)
    }

    override fun onResume() {
        super.onResume()

        // kick-off recording process, if we're allowed
        if (permissionToRecord) {
            launch(Dispatchers.Main) {
                // start streaming the data to the server and collect responses
                val streams = stub.prepare {
                    withInitialRequest(StreamingRecognizeRequest.newBuilder().apply {
                        streamingConfig = StreamingRecognitionConfig.newBuilder().apply {
                            config = RecognitionConfig.newBuilder().apply {
                                languageCode = "en-US"
                                encoding = RecognitionConfig.AudioEncoding.LINEAR16
                                sampleRateHertz = 16000
                            }.build()
                            interimResults = false
                            singleUtterance = false
                        }.build()
                    }.build())
                }.executeStreaming { it::streamingRecognize }

                // monitor the input stream and send requests as audio data becomes available
                launch(Dispatchers.IO) {
                    for (bytes in audioEmitter.start(this)) {
                        streams.requests.send(
                            StreamingRecognizeRequest.newBuilder().apply {
                                audioContent = bytes
                            }.build()
                        )
                    }
                }

                // stop after a few seconds
                launch {
                    delay(5_000)
                    streams.requests.close()
                }

                // handle incoming responses
                for (response in streams.responses) {
                    updateUIWithExampleResult(response.toString())
                }
            }
        } else {
            Log.e(TAG, "No permission to record! Please allow and then relaunch the app!")
        }
    }

    override fun onPause() {
        super.onPause()

        // ensure mic data stops
        audioEmitter.stop()
    }

    override fun onRequestPermissionsResult(
        requestCode: Int,
        permissions: Array<String>,
        grantResults: IntArray
    ) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)

        when (requestCode) {
            REQUEST_RECORD_AUDIO_PERMISSION ->
                permissionToRecord = grantResults[0] == PackageManager.PERMISSION_GRANTED
        }

        if (!permissionToRecord) {
            finish()
        }
    }
}
