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
import com.google.cloud.speech.v1.RecognitionConfig
import com.google.cloud.speech.v1.SpeechGrpc
import com.google.cloud.speech.v1.StreamingRecognitionConfig
import com.google.cloud.speech.v1.StreamingRecognizeRequest
import com.google.protobuf.ByteString
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayOutputStream
import java.io.File
import javax.sound.sampled.AudioFormat
import javax.sound.sampled.AudioSystem
import javax.sound.sampled.DataLine
import javax.sound.sampled.TargetDataLine

/**
 * Simple example of calling the Speech API with KGax.
 *
 * Run this example using your service account as follows:
 *
 * ```
 * $ CREDENTIALS=<path_to_your_service_account.json> ./gradlew examples:run --args speech
 * ```
 */
@ExperimentalCoroutinesApi
fun speechStreamingExample(credentials: String) = runBlocking {
    // create a stub factory
    val factory = StubFactory(
        SpeechGrpc.SpeechStub::class, "speech.googleapis.com", 443
    )

    // create a stub
    val stub = File(credentials).inputStream().use {
        factory.fromServiceAccount(it, listOf("https://www.googleapis.com/auth/cloud-platform"))
    }

    // call the API and get the input & output streams
    val streams = stub
        .prepare {
            withInitialRequest(
                StreamingRecognizeRequest.newBuilder().apply {
                    streamingConfig = StreamingRecognitionConfig.newBuilder().apply {
                        config = RecognitionConfig.newBuilder().apply {
                            languageCode = "en-US"
                            encoding = RecognitionConfig.AudioEncoding.LINEAR16
                            sampleRateHertz = 16000
                        }.build()
                        interimResults = false
                        singleUtterance = false
                    }.build()
                }.build()
            )
        }
        .executeStreaming {
            it::streamingRecognize
        }

    // send some speech to the server in a background job
    launch {
        for (data in getAudio()) {
            streams.requests.send(
                StreamingRecognizeRequest.newBuilder().apply {
                    audioContent = ByteString.copyFrom(data)
                }.build()
            )
        }

        // close the request stream since we're done sending
        streams.requests.close()

        // keep the program alive a little longer so we get all the responses
        delay(1_000)
    }

    // print the results as they become available
    println("Please start talking...\n")
    for (response in streams.responses) {
        println("You said: '${response.resultsList}'\n")
    }
    println("Thanks, please stop talking!")

    // shutdown all connections
    factory.shutdown()
}

// get a 100ms chunk of audio data from the microphone
@ExperimentalCoroutinesApi
private fun CoroutineScope.getAudio(
    durationInSeconds: Int = 10,
    format: AudioFormat = AudioFormat(16000.0f, 16, 1, true, false)
) = produce {
    val info = DataLine.Info(TargetDataLine::class.java, format)
    if (!AudioSystem.isLineSupported(info)) {
        throw IllegalStateException("Audio format not supported")
    }

    val line = AudioSystem.getLine(info) as TargetDataLine
    line.open(format)
    line.start()

    val now = System.currentTimeMillis()
    while (System.currentTimeMillis() - now < durationInSeconds * 1_000) {
        var numRead = 0
        val data = ByteArrayOutputStream()
        val chunk = ByteArray(line.bufferSize / 5)
        while (numRead < chunk.size) {
            val read = line.read(chunk, numRead, chunk.size - numRead)
            data.write(chunk, numRead, read)
            numRead += read
        }
        send(data.toByteArray())
    }

    line.stop()
}