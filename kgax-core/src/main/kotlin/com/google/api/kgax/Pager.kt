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

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope

/** Contains the [elements] of a page and a [token] to get the next set of results. */
interface Page<T, K> {
    val elements: Iterable<T>
    val token: K?
}

/**
 * Create a stream of [Page]s by calling the [method] (once per page) given the [initialRequest] and a set
 * of lambdas. The [nextRequest] lambda is used to transform the initial request to a new request for
 * subsequent pages using the given page token. The [nextPage] lambda is used to extract the result list,
 * next page token, and any other arbitrary metadata after a new page is fetched.
 */
@ExperimentalCoroutinesApi
suspend fun <ReqT, RespT, ElementT, TokenT, PageT : Page<ElementT, TokenT>> createPager(
    method: suspend (ReqT) -> RespT,
    initialRequest: () -> ReqT,
    nextRequest: (ReqT, TokenT) -> ReqT,
    nextPage: (RespT) -> PageT,
    hasNextPage: (PageT) -> Boolean = { p -> p.elements.any() && p.token != null }
): ReceiveChannel<PageT> = coroutineScope {
    produce(capacity = Channel.UNLIMITED) {
        val original = initialRequest()

        // iterate through all requests
        var request: ReqT? = original
        while (request != null) {
            val page = nextPage(method(request))
            send(page)

            // get next request
            request = if (hasNextPage(page)) nextRequest(original, page.token!!) else null
        }
    }
}
