package com.google.api.kgax.examples.grpc

import android.os.Bundle
import android.support.test.espresso.idling.CountingIdlingResource
import android.support.v7.app.AppCompatActivity
import com.google.api.kgax.grpc.GrpcClientStub
import com.google.api.kgax.grpc.StubFactory
import io.grpc.stub.AbstractStub
import kotlinx.android.synthetic.main.activity_main.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlin.coroutines.CoroutineContext

/**
 * Base class for the examples.
 */
abstract class AbstractExampleActivity<T : AbstractStub<T>>(
    val idler: CountingIdlingResource
) : AppCompatActivity(), CoroutineScope {

    protected abstract val factory: StubFactory<T>
    protected abstract val stub: GrpcClientStub<T>

    lateinit var job: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Main + job

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        job = Job()

        idler.increment()
    }

    protected fun updateUIWithExampleResult(text: String) {
        resultText.text = text

        if (!idler.isIdleNow) {
            idler.decrement()
        }
    }

    override fun onDestroy() {
        super.onDestroy()

        job.cancel()

        // clean up
        factory.shutdown()
    }
}