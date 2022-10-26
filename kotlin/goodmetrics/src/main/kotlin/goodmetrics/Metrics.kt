package goodmetrics

import goodmetrics.pipeline.bucketBase2

enum class MetricsBehavior {
    DEFAULT,
    NO_TOTALTIME // don't include a `totaltime` timeseries for the metric
}

/**
 * Not thread safe.
 */
data class Metrics internal constructor(
    internal val name: String,
    internal var timestampNanos: Long,
    internal val startNanoTime: Long,
    internal val metricsBehavior: MetricsBehavior = MetricsBehavior.DEFAULT,
) {
    sealed interface Dimension {
        val name: kotlin.String
        data class String(override val name: kotlin.String, val value: kotlin.String) : Dimension
        data class Number(override val name: kotlin.String, val value: Long) : Dimension
        data class Boolean(override val name: kotlin.String, val value: kotlin.Boolean) : Dimension
    }
    internal val metricMeasurements: MutableMap<String, Number> = mutableMapOf()
    internal val metricDistributions: MutableMap<String, Long> = mutableMapOf()
    internal val metricDimensions: MutableMap<String, Dimension> = mutableMapOf()

    data class View(
        val metricName: String,
        val timestampNanos: Long,
        val startNanoTime: Long,
        val dimensions: Map<String, Dimension>,
        val measurements: Map<String, Number>,
        val distributions: Map<String, Long>,
    )

    fun getView(): View {
        return View(
            name,
            timestampNanos,
            startNanoTime,
            metricDimensions.toMap(),
            metricMeasurements.toMap(),
            metricDistributions.toMap(),
        )
    }

    fun dimension(dimension: String, value: Boolean) {
        metricDimensions[dimension] = Dimension.Boolean(dimension, value)
    }

    fun dimension(dimension: String, value: Long) {
        metricDimensions[dimension] = Dimension.Number(dimension, value)
    }

    fun dimension(dimension: String, value: String) {
        metricDimensions[dimension] = Dimension.String(dimension, value)
    }

    /**
     * Record a dimension with a position based on log2(value).
     * This method handles bucketing, just pass in a bytes count or whatever you need.
     *
     * This is a potentially expensive thing to use - you should carefully consider if
     * you should use a distribution() instead. This will cause log2(maxvalue - minvalue)
     * additional cardinality in your metrics. If you're using Goodmetrics and Timescale
     * that's not a big deal - you'll buffer in memory for $reportInterval and ship it on.
     * If you're using lightstep or something like that it's a factor to consider.
     *
     * You WANT to use this if you are trying to plot a latency BY request size.
     * You DO NOT WANT to use this if you are trying to plot a latency or plot a
     * request size.
     *
     * Positive values only.
     */
    fun dimensionBase2Bucketed(dimension: String, value: Long) {
        metricDimensions[dimension] = Dimension.Number(dimension, bucketBase2(value))
    }

    fun measure(name: String, value: Long) {
        metricMeasurements[name] = value
    }

    fun measure(name: String, value: Int) {
        metricMeasurements[name] = value.toLong()
    }

    fun measure(name: String, value: Double) {
        metricMeasurements[name] = value
    }

    fun measure(name: String, value: Float) {
        metricMeasurements[name] = value.toDouble()
    }

    /**
     * Distributions are positive only.
     * This only records 1 position of a distribution per Metrics lifetime.
     */
    fun distribution(name: String, value: Long) {
        if (value < 0) {
            return
        }
        metricDistributions[name] = value
    }

    /**
     * Distributions are positive only.
     * This only records 1 position of a distribution per Metrics lifetime.
     */
    fun distribution(name: String, value: Int) {
        if (value < 0) {
            return
        }
        metricDistributions[name] = value.toLong()
    }
}
