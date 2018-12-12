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

package com.google.api.kgax

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.Test

private data class RequestType(val query: Int, val token: String? = null)
private data class ResponseType(val items: List<String>, val token: String? = null)

private data class TestPage(
    override val elements: Iterable<String>,
    override val token: String? = null
) : Page<String, String>

class PagerTest {

    @Test
    fun `Pages through data`() = runBlocking<Unit> {
        val request = RequestType(22)
        suspend fun method(request: RequestType) = withContext(Dispatchers.Default) {
            when (request.token) {
                null -> ResponseType(listOf("one", "two"), "first")
                "first" -> ResponseType(listOf("three", "four"), "second")
                else -> ResponseType(listOf("five", "six"))
            }
        }

        var count = 0
        val pager =
            createPager(
                method = ::method,
                initialRequest = {
                    request
                },
                nextRequest = { request, token ->
                    assertThat(request).isEqualTo(request)
                    RequestType(request.query, token)
                },
                nextPage = { response ->
                    count++
                    TestPage(response.items, response.token)
                }
            )

        val results = mutableListOf<String>()
        for (page in pager) {
            for (entry in page.elements) {
                results.add(entry)
            }
        }
        assertThat(results).containsExactly("one", "two", "three", "four", "five", "six").inOrder()
    }

    @Test
    fun `Ends when no data`() = runBlocking<Unit> {
        suspend fun method(request: RequestType) = withContext(Dispatchers.Default) { ResponseType(listOf()) }

        val pager =
            createPager(
                method = ::method,
                initialRequest = {
                    RequestType(2)
                },
                nextRequest = { request, token ->
                    RequestType(request.query, token)
                },
                nextPage = { response ->
                    TestPage(response.items, response.token)
                }
            )

        val results = mutableListOf<String>()
        for (page in pager) {
            for (entry in page.elements) {
                results.add(entry)
            }
        }
        assertThat(results).isEmpty()
    }

    @Test
    fun `Ends when no token`() = runBlocking<Unit> {
        suspend fun method(request: RequestType) = withContext(Dispatchers.Default) { ResponseType(listOf("one")) }

        val pager =
            createPager(
                method = ::method,
                initialRequest = {
                    RequestType(2)
                },
                nextRequest = { request, token ->
                    RequestType(request.query, token)
                },
                nextPage = { response ->
                    TestPage(response.items)
                }
            )

        val results = mutableListOf<String>()
        for (page in pager) {
            for (entry in page.elements) {
                results.add(entry)
            }
        }
        assertThat(results).containsExactly("one")
    }
}