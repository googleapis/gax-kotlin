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

import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * A Pager that can fetch results from a paged API on demand.
 *
 * *Note*: This is for simple use cases. Consider using Android's [android.arch.paging.PagedList]
 * if it's available in your application as well.
 */
class Pager<ReqT, RespT, ElementT> internal constructor(
        private val config: PagerConfig<ReqT, RespT, ElementT>
) : AbstractIterator<Page<ElementT>>() {

    private var token: String? = null
    private var original: ReqT? = null
    private var hasNext = true

    /**
     * Iterate through the list one page at a time. Using this method will prevent
     * the current thread from being blocked when pages are fetched. Page fetches will
     * occur on [Pager.executor] and the [handler] will be processed on the given [executor].
     */
    fun forEach(executor: Executor?, handler: (Page<ElementT>) -> Unit) {
        val fetchExecutor = Pager.executor
        val responseExecutor = executor

        fun fetch() {
            fetchExecutor.execute {
                val page = fetchNextPage()
                if (page != null) {
                    val action = {
                        handler(page)
                        fetch()
                    }
                    responseExecutor?.execute { action() } ?: action()
                }
            }
        }
        fetch()
    }

    /** Invokes the [handler] via [Pager.executor]. */
    fun forEach(handler: (Page<ElementT>) -> Unit) = forEach(null, handler)

    private fun fetchNextPage(): Page<ElementT>? {
        // check for end of list
        if (!hasNext) {
            done()
            return null
        }

        // create request
        if (original == null) {
            original = config.initialRequest()
        }
        val req = token?.let {
            config.nextRequest(original!!, it)
        } ?: original!!

        // get next set of results and the next page token
        val page = config.nextPage(config.method(req))
        token = page.token
        hasNext = !token.isNullOrBlank() && page.elements.any()

        return page
    }

    override fun computeNext() {
        val page = fetchNextPage()

        // set next
        if (page != null && page.elements.any()) {
            setNext(page)
        }
    }

    companion object {
        /** The executor to use for fetching pages via the [onPage] method. */
        var executor: ExecutorService = Executors.newCachedThreadPool()
    }
}

/** Contains the [elements] of a page fetched with the [token] and arbitrary [metadata]. */
interface Page<T> {
    val elements: Iterable<T>
    val token: String?
    val metadata: Any?
}

/**
 * Create a [Pager].
 *
 * ```
 * val pager = pager<ListLogEntriesRequest, ListLogEntriesResponse, LogEntry> {
 *      method = stub::listLogEntries
 *      initialRequest = {
 *          ListLogEntriesRequest.newBuilder()
 *                  .addResourceNames(project)
 *                  .setFilter("logName=$log")
 *                  .setPageSize(10)
 *                  .build()
 *      }
 *      nextRequest = { request, token ->
 *          request.toBuilder().setPageToken(token).build()
 *      }
 *      nextPage = { response ->
 *          PageResult(response.entriesList, response.nextPageToken)
 *      }
 *  }
 *
 *  // go through all pages
 *  for (page in pager) {
 *      for (entry in page.elements) {
 *          println(entry.textPayload)
 *      }
 *  }
 * ```
 */
fun <ReqT, RespT, ElementT> pager(init: PagerConfig<ReqT, RespT, ElementT>.() -> Unit
): Pager<ReqT, RespT, ElementT> {
    val config = PagerConfig<ReqT, RespT, ElementT>()
    config.init()
    return Pager(config)
}

@DslMarker
annotation class PageMarker

/**
 * [Pager] configuration for a paged API method call with input type [RespT] and return type [RespT].
 */
@PageMarker
class PagerConfig<ReqT, RespT, ElementT> {

    /** A method to call (once per page) */
    lateinit var method: (ReqT) -> RespT

    /** The initial request object (i.e. passed as input to [method] to fetch the first page). */
    lateinit var initialRequest: () -> ReqT

    /** A lambda to transform the initial request to a new request for subsequent pages using the given page token. */
    lateinit var nextRequest: (ReqT, String) -> ReqT

    /** A lambda to extract the result list, next page token, and any other arbitrary metadata after a new page is fetched. */
    lateinit var nextPage: (RespT) -> Page<ElementT>

}
