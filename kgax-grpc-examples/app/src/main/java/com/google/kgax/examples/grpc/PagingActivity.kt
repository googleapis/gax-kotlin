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
import android.widget.TextView
import com.google.api.MonitoredResource
import com.google.kgax.Page
import com.google.kgax.ServiceAccount
import com.google.kgax.grpc.GrpcClientStub
import com.google.kgax.grpc.StubFactory
import com.google.kgax.pager
import com.google.logging.v2.ListLogEntriesRequest
import com.google.logging.v2.ListLogEntriesResponse
import com.google.logging.v2.LogEntry
import com.google.logging.v2.LoggingServiceV2Grpc
import com.google.logging.v2.WriteLogEntriesRequest
import java.util.Date

/**
 * Kotlin example showcasing paging using KGax with gRPC.
 *
 * @author jbolinger
 */
class PagingActivity : AppCompatActivity() {

    private val factory = StubFactory(
            LoggingServiceV2Grpc.LoggingServiceV2BlockingStub::class,
            "logging.googleapis.com")

    private val stub by lazy {
        applicationContext.resources.openRawResource(R.raw.sa).use {
            factory.fromServiceAccount(it,
                    listOf("https://www.googleapis.com/auth/logging.write",
                            "https://www.googleapis.com/auth/logging.admin",
                            "https://www.googleapis.com/auth/logging.read",
                            "https://www.googleapis.com/auth/cloud-platform"))
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        // get the project id
        // in a real app you wouldn't need to do this and could use a constant string
        val projectId = applicationContext.resources.openRawResource(R.raw.sa).use {
            ServiceAccount.getProjectFromKeyFile(it)
        }

        val resultText: TextView = findViewById(R.id.result_text)

        // call the api
        ApiTestTask(stub, projectId) { resultText.text = it }.execute()
    }

    override fun onDestroy() {
        super.onDestroy()

        // clean up
        factory.shutdown()
    }

    private class ApiTestTask(
            val stub: GrpcClientStub<LoggingServiceV2Grpc.LoggingServiceV2BlockingStub>,
            val projectId: String,
            val onResult: (String) -> Unit
    ) : AsyncTask<Unit, Unit, String>() {
        override fun doInBackground(vararg params: Unit): String {
            val output = StringBuffer("\n")

            // resources to use
            val project = "projects/$projectId"
            val log = "$project/logs/testLog-${Date().time}"

            // ensure we have some logs to read
            val writeRequest = WriteLogEntriesRequest.newBuilder()
            for (i in 1..40) {
                writeRequest.addEntries(LogEntry.newBuilder()
                        .setResource(MonitoredResource.newBuilder().setType("global").build())
                        .setLogName(log)
                        .setTextPayload("log number: $i")
                        .build())
            }
            stub.executeBlocking { it.writeLogEntries(writeRequest.build()) }

            // create a pager
            val pager = pager<ListLogEntriesRequest, ListLogEntriesResponse, LogEntry> {
                method = { request ->
                    stub.executeBlocking { it.listLogEntries(request) }.body
                }
                initialRequest = {
                    ListLogEntriesRequest.newBuilder()
                            .addResourceNames(project)
                            .setFilter("logName=$log")
                            .setPageSize(10)
                            .build()
                }
                nextRequest = { request, token ->
                    request.toBuilder().setPageToken(token).build()
                }
                nextPage = { response ->
                    LogEntryPage(response.entriesList, response.nextPageToken)
                }
            }

            // go through all pages
            for (page in pager) {
                for (entry in page.elements) {
                    output.append("log : ${entry.textPayload}\n")
                }
            }

            // done!
            return output.toString()
        }

        override fun onPostExecute(result: String) {
            onResult(result)
        }
    }
}

private data class LogEntryPage(override val elements: Iterable<LogEntry>,
                                override val token: String?,
                                override val metadata: Unit? = null) : Page<LogEntry>