package goodmetrics

import goodmetrics.pipeline.MetricsSink
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

internal class MetricsFactoryTest {
    private val emittedMetrics: MutableList<Metrics> = mutableListOf()
    private val testMetricsSink = object : MetricsSink {
        override fun emit(metrics: Metrics) {
            emittedMetrics.add(metrics)
        }

        override fun close() {
            // nothing to do here
        }
    }
    private var nowNanos = 0L

    @BeforeTest
    fun before() {
        nowNanos = 0
        emittedMetrics.clear()
    }

    @Test
    fun testDistributionTotaltimeType() {
        val metricsFactory = MetricsFactory(
            sink = testMetricsSink,
            timeSource = { nowNanos },
            totaltimeType = MetricsFactory.TotaltimeType.DistributionMicroseconds
        )

        metricsFactory.record("test") { metrics ->
            metrics.dimension("a_dimension", "a")
            metrics.measure("a_measurement", 0)
            metrics.distribution("a_distribution", 1)
        }
        assertEquals(1, emittedMetrics.size)
        val metric = emittedMetrics[0]
        metric.assertPresence(
            dimensions = setOf("a_dimension"),
            measurements = setOf("a_measurement"),
            distributions = setOf("totaltime", "a_distribution")
        )
    }

    @Test
    fun testDistributionTotaltimeTypeNoTotaltimeBehavior() {
        val metricsFactory = MetricsFactory(
            sink = testMetricsSink,
            timeSource = { nowNanos },
            totaltimeType = MetricsFactory.TotaltimeType.DistributionMicroseconds
        )

        metricsFactory.recordWithBehavior("test", metricsBehavior = MetricsBehavior.NO_TOTALTIME) { metrics ->
            metrics.dimension("a_dimension", "a")
            metrics.measure("a_measurement", 0)
            metrics.distribution("a_distribution", 1)
        }
        assertEquals(1, emittedMetrics.size)
        val metric = emittedMetrics[0]
        metric.assertPresence(
            dimensions = setOf("a_dimension"),
            measurements = setOf("a_measurement"),
            distributions = setOf("a_distribution")
        )
    }

    @Test
    fun testMeasurementTotaltimeType() {
        val metricsFactory = MetricsFactory(
            sink = testMetricsSink,
            timeSource = { nowNanos },
            totaltimeType = MetricsFactory.TotaltimeType.MeasurementMicroseconds
        )

        metricsFactory.record("test") { metrics ->
            metrics.dimension("a_dimension", "a")
            metrics.measure("a_measurement", 0)
            metrics.distribution("a_distribution", 1)
        }
        assertEquals(1, emittedMetrics.size)
        val metric = emittedMetrics[0]
        metric.assertPresence(
            dimensions = setOf("a_dimension"),
            measurements = setOf("totaltime", "a_measurement"),
            distributions = setOf("a_distribution")
        )
    }

    @Test
    fun testMeasurementTotaltimeTypeNoTotaltimeBehavior() {
        val metricsFactory = MetricsFactory(
            sink = testMetricsSink,
            timeSource = { nowNanos },
            totaltimeType = MetricsFactory.TotaltimeType.MeasurementMicroseconds
        )

        metricsFactory.recordWithBehavior("test", metricsBehavior = MetricsBehavior.NO_TOTALTIME) { metrics ->
            metrics.dimension("a_dimension", "a")
            metrics.measure("a_measurement", 0)
            metrics.distribution("a_distribution", 1)
        }
        assertEquals(1, emittedMetrics.size)
        val metric = emittedMetrics[0]
        metric.assertPresence(
            dimensions = setOf("a_dimension"),
            measurements = setOf("a_measurement"),
            distributions = setOf("a_distribution")
        )
    }

    @Test
    fun testNoTotaltimeType() {
        val metricsFactory = MetricsFactory(
            sink = testMetricsSink,
            timeSource = { nowNanos },
            totaltimeType = MetricsFactory.TotaltimeType.None
        )

        metricsFactory.record("test") { metrics ->
            metrics.dimension("a_dimension", "a")
            metrics.measure("a_measurement", 0)
            metrics.distribution("a_distribution", 1)
        }
        assertEquals(1, emittedMetrics.size)
        val metric = emittedMetrics[0]
        metric.assertPresence(
            dimensions = setOf("a_dimension"),
            measurements = setOf("a_measurement"),
            distributions = setOf("a_distribution")
        )
    }
}

fun Metrics.assertPresence(dimensions: Set<String>, measurements: Set<String>, distributions: Set<String>) {
    val view = getView()
    assertEquals(dimensions, view.dimensions.keys)
    assertEquals(measurements, view.measurements.keys)
    assertEquals(distributions, view.distributions.keys)
}
