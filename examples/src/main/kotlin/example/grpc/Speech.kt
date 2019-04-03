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
import com.google.api.kgax.grpc.executeLongRunning
import com.google.cloud.speech.v1.LongRunningRecognizeRequest
import com.google.cloud.speech.v1.LongRunningRecognizeResponse
import com.google.cloud.speech.v1.RecognitionAudio
import com.google.cloud.speech.v1.RecognitionConfig
import com.google.cloud.speech.v1.SpeechGrpc
import com.google.common.io.ByteStreams
import com.google.protobuf.ByteString
import example.Main
import kotlinx.coroutines.runBlocking
import java.io.File

/**
 * Simple example of calling the Speech API with KGax.
 *
 * Run this example using your service account as follows:
 *
 * ```
 * $ CREDENTIALS=<path_to_your_service_account.json> ./gradlew examples:run --args speech
 * ```
 */
fun speechExample(credentials: String) = runBlocking {
    // create a stub factory
    val factory = StubFactory(
        SpeechGrpc.SpeechFutureStub::class, "speech.googleapis.com", 443
    )

    // create a stub
    val stub = File(credentials).inputStream().use {
        factory.fromServiceAccount(it, listOf("https://www.googleapis.com/auth/cloud-platform"))
    }

    // get some audio to use
    val audioData = Main::class.java.getResourceAsStream("/audio.raw").use {
        ByteString.copyFrom(ByteStreams.toByteArray(it))
    }

    // call the API
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
    println("Waiting for long running operation...")
    val response = lro.await()

    println("Operation completed: ${lro.operation?.name} with result:\n$response")

    // shutdown all connections
    factory.shutdown()
}
