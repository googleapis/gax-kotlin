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
import com.google.api.kgax.grpc.StreamingCall
import com.google.api.kgax.grpc.StubFactory
import com.google.cloud.speech.v1.RecognitionConfig
import com.google.cloud.speech.v1.SpeechGrpc
import com.google.cloud.speech.v1.StreamingRecognitionConfig
import com.google.cloud.speech.v1.StreamingRecognizeRequest
import com.google.cloud.speech.v1.StreamingRecognizeResponse
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.coroutines.CoroutineContext

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
    lateinit var job: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Main + job

    private var permissionToRecord = false
    private val audioEmitter: AudioEmitter = AudioEmitter()
    private var streams: SpeechStreams? = null

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

        job = Job()

        // kick-off recording process, if we're allowed
        if (permissionToRecord) {
            launch { streams = transcribe() }
        }
    }

    override fun onPause() {
        super.onPause()

        // ensure mic data stops
        runBlocking {
            audioEmitter.stop()
            streams?.responses?.cancel()
        }

        job.cancel()
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
            Log.e(TAG, "No permission to record - please grant and retry!")
            finish()
        }
    }

    private suspend fun transcribe(): SpeechStreams {
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

        // handle incoming responses
        launch(Dispatchers.Main) {
            for (response in streams.responses) {
                updateUIWithExampleResult(response.toString())
            }
        }

        return streams
    }
}

private typealias SpeechStreams = StreamingCall<StreamingRecognizeRequest, StreamingRecognizeResponse>
