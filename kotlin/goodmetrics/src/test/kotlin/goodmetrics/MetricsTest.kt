package goodmetrics

import goodmetrics.pipeline.MetricsSink
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

internal class MetricsTest {
    private lateinit var metricsFactory: MetricsFactory
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
        metricsFactory = MetricsFactory(sink = testMetricsSink, timeSource = { nowNanos }, totaltimeType = MetricsFactory.TotaltimeType.DistributionMicroseconds)
    }

    @Test
    fun testTimestampStart() {
        nowNanos = 13
        val metrics = metricsFactory.internals.getMetrics("test", TimestampAt.Start)
        metrics.assert(timestamp = 13)
    }

    @Test
    fun testTimestampEnd() {
        nowNanos = 13
        val metrics = metricsFactory.internals.getMetrics("test", TimestampAt.End)
        metrics.assert(timestamp = -1)

        nowNanos = 17
        metricsFactory.internals.emit(metrics)
        metrics.assert(timestamp = 17)
    }

    @Test
    fun testEmitEmits() {
        val metrics = metricsFactory.internals.getMetrics("test", TimestampAt.End)
        assertEquals(listOf(), emittedMetrics)

        metricsFactory.internals.emit(metrics)
        assertEquals(listOf(metrics), emittedMetrics)
    }

    @Test
    fun testRecordEmits() {
        // Don't ever let metrics references escape a record block!
        // This is for a test assertion only.
        var illegalUsage: Metrics
        metricsFactory.record("test") { metrics ->
            illegalUsage = metrics
        }

        assertEquals(listOf(illegalUsage), emittedMetrics)
    }

    @Test
    fun testDimensionOverloads() {
        val metrics = metricsFactory.internals.getMetrics("test", TimestampAt.End)
        metrics.dimension("1", true)
        metrics.dimension("2", false)
        metrics.dimension("3", 12L)
        metrics.dimension("4", 17L)
        metrics.dimension("5", "a")
        metrics.dimension("6", "b")
        metrics.dimensionBase2Bucketed("7", 42)
        metrics.assert(
            dimensions = mapOf(
                "1" to Metrics.Dimension.Boolean("1", true),
                "2" to Metrics.Dimension.Boolean("2", false),
                "3" to Metrics.Dimension.Number("3", 12),
                "4" to Metrics.Dimension.Number("4", 17),
                "5" to Metrics.Dimension.String("5", "a"),
                "6" to Metrics.Dimension.String("6", "b"),
                "7" to Metrics.Dimension.Number("7", 64),
            )
        )
    }

    @Test
    fun testMeasureOverloads() {
        val metrics = metricsFactory.internals.getMetrics("test", TimestampAt.End)
        metrics.measure("1", 1L)
        metrics.measure("2", 2L)
        metrics.measure("3", 3)
        metrics.measure("4", 4)
        metrics.measure("5", 5.125)
        metrics.measure("6", 6.250)
        metrics.measure("7", 7.375f)
        metrics.measure("8", 8.500f)
        metrics.assert(
            measurements = listOf(
                "1" to 1L,
                "2" to 2L,
                // Ints are treated as longs in goodmetrics
                "3" to 3L,
                "4" to 4L,
                "5" to 5.125,
                "6" to 6.250,
                // Floats are treated as Doubles in goodmetrics
                "7" to 7.375,
                "8" to 8.500,
            )
        )
    }

    @Test
    fun testDistributionOverloads() {
        val metrics = metricsFactory.internals.getMetrics("test", TimestampAt.End)
        metrics.distribution("1", 1L)
        metrics.distribution("2", 2L)
        metrics.distribution("3", 3)
        metrics.distribution("4", 4)
        metrics.assert(
            distributions = mapOf(
                "1" to 1L,
                "2" to 2L,
                "3" to 3,
                "4" to 4,
            )
        )
    }
}

fun Metrics.assert(timestamp: Long? = null, dimensions: Map<String, Metrics.Dimension>? = null, measurements: List<Pair<String, Number>>? = null, distributions: Map<String, Long>? = null) {
    val view = getView()
    if (timestamp != null) {
        assertEquals(timestamp, view.timestampNanos)
    }
    if (dimensions != null) {
        assertEquals(dimensions, view.dimensions)
    }
    if (measurements != null) {
        measurements.forEach { (name, value) -> assertEquals(value, view.measurements[name]) }
        assertEquals(measurements.size, view.measurements.size)
    }
    if (distributions != null) {
        assertEquals(distributions, view.distributions)
    }
}
