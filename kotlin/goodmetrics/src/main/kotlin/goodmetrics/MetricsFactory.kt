@file:OptIn(ExperimentalContracts::class, ExperimentalContracts::class)

package goodmetrics

import goodmetrics.pipeline.MetricsSink
import java.time.Instant
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

enum class TimestampAt {
    // Stamp the metric at the start
    Start,

    // Stamp the metric at the end
    End,
}

fun interface NanoTimeSource {
    fun epochNanos(): Long

    companion object {
        val preciseNanoTime = NanoTimeSource {
            val now = Instant.now()
            now.epochSecond * 1000000000 + now.nano
        }

        val fastNanoTime = NanoTimeSource {
            System.currentTimeMillis() * 1000000
        }
    }
}

class MetricsFactory(
    private val sink: MetricsSink,
    @PublishedApi internal val timeSource: NanoTimeSource,
    private val totaltimeType: TotaltimeType
) : AutoCloseable {
    /**
     * Tools for making abstractions around the metrics factory other than the usual record{} pattern.
     */
    val internals: Internals = Internals(this)
    enum class TotaltimeType {
        /**
         * totaltime is a histogram (preferred)
         */
        DistributionMicroseconds,

        /**
         * totaltime is a statisticset when preaggregated
         */
        MeasurementMicroseconds,

        /**
         * No totaltime metric
         */
        None,
    }

    /**
     * Passes a Metrics into your scope. Record your unit of work; when the scope exits
     * the Metrics will be stamped with `totaltime` and emitted through the pipeline.
     */
    inline fun <T> record(name: String, stampAt: TimestampAt = TimestampAt.Start, block: (Metrics) -> T): T {
        contract {
            callsInPlace(block, InvocationKind.EXACTLY_ONCE)
        }
        return recordWithBehavior(name, stampAt, MetricsBehavior.DEFAULT, block)
    }

    /**
     * Passes a Metrics into your scope. Record your unit of work; when the scope exits
     * the Metrics will be stamped with `totaltime` and emitted through the pipeline.
     *
     * Allows for setting specific MetricsBehaviors for the metric.
     *
     * If you don't want `totaltime` timeseries data, then specify `metricBehavior: MetricBehavior.NO_TOTALTIME`.
     */
    inline fun <T> recordWithBehavior(name: String, stampAt: TimestampAt = TimestampAt.Start, metricsBehavior: MetricsBehavior, block: (Metrics) -> T): T {
        contract {
            callsInPlace(block, InvocationKind.EXACTLY_ONCE)
        }
        val metrics = internals.getMetrics(name, stampAt, metricsBehavior)
        try {
            return block(metrics)
        } finally {
            internals.emit(metrics)
        }
    }

    /**
     * Tools for making abstractions around the metrics factory other than the usual record{} pattern.
     */
    class Internals internal constructor(private val self: MetricsFactory) {
        /**
         * For every getMetrics(), you need to also emit() that Metrics object via this same MetricsFactory.
         */
        fun getMetrics(name: String, stampAt: TimestampAt, metricsBehavior: MetricsBehavior = MetricsBehavior.DEFAULT): Metrics {
            val timestamp = when (stampAt) {
                TimestampAt.Start -> self.timeSource.epochNanos()
                TimestampAt.End -> -1
            }
            return Metrics(name, timestamp, System.nanoTime(), metricsBehavior)
        }

        /**
         * Complete and release a Metrics to the configured downstream sink.
         * If you don't emit() the metrics it will never show up downstream.
         */
        fun emit(metrics: Metrics) {
            self.finalizeMetrics(metrics)
            self.sink.emit(metrics)
        }
    }

    private fun finalizeMetrics(metrics: Metrics) {
        if (metrics.timestampNanos < 1) {
            metrics.timestampNanos = timeSource.epochNanos()
        }
        if (metrics.metricsBehavior == MetricsBehavior.NO_TOTALTIME) {
            return
        }
        val duration = System.nanoTime() - metrics.startNanoTime
        when (totaltimeType) {
            TotaltimeType.DistributionMicroseconds -> metrics.distribution("totaltime", duration / 1000)
            TotaltimeType.MeasurementMicroseconds -> metrics.measure("totaltime", duration / 1000)
            TotaltimeType.None -> {}
        }
    }

    override fun close() {
        sink.close()
    }
}
