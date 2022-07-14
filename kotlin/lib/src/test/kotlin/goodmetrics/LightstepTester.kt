package goodmetrics

import goodmetrics.MetricsSetups.Companion.lightstepNativeOtlp
import goodmetrics.downstream.OpentelemetryClient
import goodmetrics.downstream.PrescientDimensions
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.util.concurrent.ThreadLocalRandom
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

fun main() {
    runBlocking {
        val configuredMetrics = lightstepNativeOtlp(
            lightstepAccessToken = System.getenv("lightsteptoken") ?: "none",
            prescientDimensions = PrescientDimensions.AsResource(
                mapOf(
                    "service.name" to Metrics.Dimension.String("service.name", "goodmetrics_test"),
                    "service.version" to Metrics.Dimension.String("service.version", OpentelemetryClient::class.java.`package`.implementationVersion ?: "dev"),
                    "host.hostname" to Metrics.Dimension.String("host.hostname", InetAddress.getLocalHost().hostName)
                )
            ),
            aggregationWidth = 10.seconds,
            logError = { message, exception -> println("metrics error: $message, ex: ${exception.message}") },
            onSendUnary = { println("sending unary batch size: ${it.size}") },
            onSendPreaggregated = { println("sending preaggregated batch size: ${it.size}") },
        )

        launch {
            runUnaryExample(configuredMetrics.unaryMetricsFactory)
        }
        launch {
            runPreaggregatedExample(configuredMetrics.preaggregatedMetricsFactory)
        }
    }
}

private suspend fun runPreaggregatedExample(metricsFactory: MetricsFactory) = coroutineScope {
    var i = 0L
    var timestamp = TimeSource.Monotonic.markNow()
    val targetPeriod = 10.milliseconds
    while (true) {
        // Run a high frequency service api. We'll call it high_frequency_api
        ++i
        try {
            metricsFactory.record("api_100hz") { metrics ->
                metrics.dimension("a_dimension", i % 8)
                metrics.distribution("random", ThreadLocalRandom.current().nextLong(4, 6))
                metrics.distribution("free_memory", Runtime.getRuntime().freeMemory())

                metrics.measure("small_random", ThreadLocalRandom.current().nextLong(4, 40))
            }
            val waitTime = -((timestamp + targetPeriod).elapsedNow())
            if (waitTime.isPositive()) {
                delay(waitTime)
            } else {
                println(waitTime)
            }
            timestamp += targetPeriod
        } catch (e: Exception) {
            println("it broke while recording")
            e.printStackTrace()
        }
    }
}
private suspend fun runUnaryExample(metricsFactory: MetricsFactory) {
    repeat(Int.MAX_VALUE) { i ->
        // Run a service api. We'll call it "example"
        try {
            metricsFactory.record("slower_example") { metrics ->
                metrics.dimension("round", i.toLong() % 8)
                metrics.measure("random", ThreadLocalRandom.current().nextFloat(0.3f, 0.97f))
                metrics.measure("free_memory", Runtime.getRuntime().freeMemory())
                delay(ThreadLocalRandom.current().nextLong(1, 13))
            }
        } catch (e: Exception) {
            println("it broke while recording")
            e.printStackTrace()
        }
        delay(200)
    }
}
