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
package example

import example.grpc.languageExample
import example.grpc.speechExample
import example.grpc.speechStreamingExample
import kotlinx.coroutines.ExperimentalCoroutinesApi

/**
 * Simple example of calling the Google Cloud APIs with a generated Kotlin gRPC client.
 *
 * Run the examples using your service account as follows:
 *
 * ```
 * $ CREDENTIALS=<path_to_your_service_account.json> ./gradlew run --args language
 * ```
 */
class Main {
    companion object {
        @ExperimentalCoroutinesApi
        @JvmStatic
        fun main(args: Array<String>) {
            val example = args.firstOrNull()?.toLowerCase()
            try {
                val credentials = System.getenv("CREDENTIALS")
                    ?: throw RuntimeException("You must set the CREDENTIALS environment variable to run this example")
                when (example) {
                    "language" -> languageExample(credentials)
                    "speech" -> speechExample(credentials)
                    "transcribe" -> speechStreamingExample(credentials)
                    else -> usage()
                }
                System.exit(0)
            } catch (t: Throwable) {
                System.err.println("Failed: $t")
            }
            System.exit(1)
        }

        private fun usage() {
            println(
                """
                |Run a Cloud API example:
                |
                |$ export CREDENTIALS=<path_to_your_service_account_keyfile>.json
                |$ export PROJECT=<name_of_your_gcp_project>
                |$ ./gradlew run --args <example_name>
                |
                |Options:
                |  <example_name>: language, logging, speech, pubsub
                |
                |Example:
                |Run the following command to start the Natural Language example:
                |$ ./gradlew run --args language
                """.trimMargin())
            System.exit(1)
        }
    }
}
