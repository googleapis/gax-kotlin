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
import android.os.CountDownTimer
import android.support.v4.app.ActivityCompat
import android.support.v7.app.AppCompatActivity
import android.util.Log
import android.widget.TextView
import com.google.cloud.speech.v1.RecognitionConfig
import com.google.cloud.speech.v1.SpeechGrpc
import com.google.cloud.speech.v1.StreamingRecognitionConfig
import com.google.cloud.speech.v1.StreamingRecognizeRequest
import com.google.api.kgax.examples.grpc.util.AudioEmitter
import com.google.api.kgax.grpc.StubFactory

private const val TAG = "APITest"

/**
 * Kotlin example showcasing streaming APIs using KGax with gRPC.
 *
 * @author jbolinger
 */
class StreamingActivity : AppCompatActivity() {

    private val PERMISSIONS = arrayOf(Manifest.permission.RECORD_AUDIO)
    private val REQUEST_RECORD_AUDIO_PERMISSION = 200

    private var permissionToRecord = false
    private var audioEmitter: AudioEmitter? = null

    private val factory = StubFactory(
            SpeechGrpc.SpeechStub::class, "speech.googleapis.com")

    private val stub by lazy {
        applicationContext.resources.openRawResource(R.raw.sa).use {
            factory.fromServiceAccount(it,
                    listOf("https://www.googleapis.com/auth/cloud-platform"))
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        // get permissions
        ActivityCompat.requestPermissions(
                this, PERMISSIONS, REQUEST_RECORD_AUDIO_PERMISSION)
    }

    override fun onResume() {
        super.onResume()

        val resultText: TextView = findViewById(R.id.result_text)

        // kick-off recording process, if we're allowed
        if (permissionToRecord) {
            audioEmitter = AudioEmitter()

            // start streaming the data to the server and collect responses
            val stream = stub.prepare {
                withInitialRequest(StreamingRecognizeRequest.newBuilder()
                        .setStreamingConfig(StreamingRecognitionConfig.newBuilder()
                                .setConfig(RecognitionConfig.newBuilder()
                                        .setLanguageCode("en-US")
                                        .setEncoding(RecognitionConfig.AudioEncoding.LINEAR16)
                                        .setSampleRateHertz(16000)
                                        .build())
                                .setInterimResults(false)
                                .setSingleUtterance(false))
                        .build())
            }.executeStreaming { it::streamingRecognize }

            // monitor the input stream and send requests as audio data becomes available
            audioEmitter!!.start { bytes ->
                stream.requests.send(StreamingRecognizeRequest.newBuilder()
                        .setAudioContent(bytes)
                        .build())
            }

            // handle incoming responses
            stream.start {
                onNext = { resultText.text = it.toString() }
                onError = { Log.e(TAG, "uh oh", it) }
                onCompleted = { Log.i(TAG, "All done!") }
            }

            object : CountDownTimer(5_000, 5_000) {
                override fun onFinish() {
                    stream.responses.close()
                }

                override fun onTick(millisUntilFinished: Long) {
                }

            }.start()
        } else {
            Log.e(TAG, "No permission to record! Please allow and then relaunch the app!")
        }
    }

    override fun onPause() {
        super.onPause()

        // ensure mic data stops
        audioEmitter?.stop()
        audioEmitter = null
    }

    override fun onDestroy() {
        super.onDestroy()

        // cleanup
        factory.shutdown()
    }

    override fun onRequestPermissionsResult(requestCode: Int,
                                            permissions: Array<String>,
                                            grantResults: IntArray) {
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
