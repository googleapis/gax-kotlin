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

package com.google.kgax

import com.google.common.truth.Truth.assertThat
import kotlin.test.Test

class PagerTest {

    data class RequestType(val query: Int, val token: String? = null)
    data class ResponseType(val items: List<String>, val token: String? = null)

    data class TestPage(override val elements: Iterable<String>,
                        override val token: String? = null,
                        override val metadata: String? = null) : Page<String>

    @Test
    fun `Pages through data`() {
        val request = RequestType(22)
        fun method(request: RequestType): ResponseType {
            return when (request.token) {
                null -> ResponseType(listOf("one", "two"), "first")
                "first" -> ResponseType(listOf("three", "four"), "second")
                else -> ResponseType(listOf("five", "six"))
            }
        }

        var count = 0
        val pager = pager<RequestType, ResponseType, String> {
                method = ::method
                initialRequest = {
                    request
                }
                nextRequest = { request, token ->
                    assertThat(request).isEqualTo(request)
                    RequestType(request.query, token)
                }
                nextPage = { response ->
                    count++
                    TestPage(response.items, response.token, "extra_$count")
                }
            }

        val results = mutableListOf<String>()
        for ((idx, page) in pager.withIndex()) {
            for (entry in page.elements) {
                results.add(entry)
            }
            assertThat(page.metadata).isEqualTo("extra_${idx+1}")
        }
        assertThat(results).containsExactly("one", "two", "three", "four", "five", "six")
    }

    @Test
    fun `Ends when no data`() {
        val pager = pager<RequestType, ResponseType, String> {
            method = { ResponseType(listOf()) }
            initialRequest = {
                RequestType(2)
            }
            nextRequest = { request, token ->
                RequestType(request.query, token)
            }
            nextPage = { response ->
                TestPage(response.items, response.token)
            }
        }

        val results = mutableListOf<String>()
        for(page in pager) {
            page.elements.forEach { results.add(it) }
            assertThat(page.metadata).isNull()
        }
        assertThat(results).isEmpty()
    }

    @Test
    fun `Ends when no token`() {
        val pager = pager<RequestType, ResponseType, String> {
            method = { ResponseType(listOf("one")) }
            initialRequest = {
                RequestType(2)
            }
            nextRequest = { request, token ->
                RequestType(request.query, token)
            }
            nextPage = { response ->
                TestPage(response.items)
            }
        }

        val results = mutableListOf<String>()
        for (page in pager) {
            page.elements.forEach { results.add(it) }
            assertThat(page.metadata).isNull()
        }
        assertThat(results).containsExactly("one")
    }
}