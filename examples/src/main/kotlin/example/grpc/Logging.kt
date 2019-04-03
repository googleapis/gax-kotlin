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

import com.google.api.MonitoredResource
import com.google.api.kgax.Page
import com.google.api.kgax.grpc.StubFactory
import com.google.api.kgax.grpc.pager
import com.google.logging.v2.ListLogEntriesRequest
import com.google.logging.v2.ListLogEntriesResponse
import com.google.logging.v2.LogEntry
import com.google.logging.v2.LoggingServiceV2Grpc
import com.google.logging.v2.WriteLogEntriesRequest
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.io.File
import java.util.Date

/**
 * Simple example of calling the Logging API with a generated Kotlin gRPC client.
 *
 * Run this example using your service account as follows:
 *
 * ```
 * $ CREDENTIALS=<path_to_your_service_account.json> PROJECT=<your_gcp_project_id> ./gradlew examples:run --args logging
 * ```
 */
@ExperimentalCoroutinesApi
fun loggingExample(credentials: String) = runBlocking {
    // create a stub factory
    val factory = StubFactory(
        LoggingServiceV2Grpc.LoggingServiceV2FutureStub::class,
        "logging.googleapis.com", 443
    )

    // create a stub
    val stub = File(credentials).inputStream().use {
        factory.fromServiceAccount(
            it,
            listOf(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/cloud-platform.read-only",
                "https://www.googleapis.com/auth/logging.admin",
                "https://www.googleapis.com/auth/logging.read",
                "https://www.googleapis.com/auth/logging.write"
            )
        )
    }

    // get the project id
    val projectId = System.getenv("PROJECT")
        ?: throw RuntimeException("You must set the PROJECT environment variable to run this example")

    // resources to use
    val project = "projects/$projectId"
    val log = "$project/logs/testLog-${Date().time}"
    val globalResource = MonitoredResource.newBuilder().apply { type = "global" }.build()

    // ensure we have some logs to read
    val newEntries = List(40) {
        LogEntry.newBuilder().apply {
            logName = log
            resource = globalResource
            textPayload = "log number: ${it + 1}"
        }.build()
    }

    // write the entries
    println("Writing ${newEntries.size} log entries...")
    stub.execute {
        it.writeLogEntries(WriteLogEntriesRequest.newBuilder().apply {
            logName = log
            resource = globalResource
            addAllEntries(newEntries)
        }.build())
    }

    // wait a few seconds (if read back immediately the API may not have saved the entries)
    delay(10_000)

    // now, read those entries back
    val pages = pager(
        method = { request ->
            stub.execute { it.listLogEntries(request) }
        },
        initialRequest = { ->
            ListLogEntriesRequest.newBuilder().apply {
                addResourceNames(project)
                filter = "logName=$log"
                orderBy = "timestamp asc"
                pageSize = 10
            }.build()
        },
        nextRequest = { request, token ->
            request.toBuilder().apply { pageToken = token }.build()
        },
        nextPage = { r: ListLogEntriesResponse ->
            Page(r.entriesList, r.nextPageToken)
        }
    )

    // go through all the logs, one page at a time
    var numRead = 0
    println("Reading log entries...")
    for (page in pages) {
        for (entry in page.elements) {
            numRead++
            println("log : ${entry.textPayload}")
        }
    }
    println("Read a total of $numRead log entries!")

    // sanity check
    if (newEntries.size != numRead) {
        throw RuntimeException("Oh no! The number of logs read does not match the number of writes!")
    }

    // shutdown
    factory.shutdown()
}
