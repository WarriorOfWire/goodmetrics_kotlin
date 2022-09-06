package goodmetrics.pipeline

import goodmetrics.Metrics
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.yield
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.DoubleAccumulator
import java.util.concurrent.atomic.DoubleAdder
import java.util.concurrent.atomic.LongAdder
import kotlin.math.ceil
import kotlin.math.log
import kotlin.math.max
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeMark

typealias MetricPosition = Set<Metrics.Dimension>
typealias MetricPositions = Map<
    /**
     * Dimensions - the position
     */
    MetricPosition,
    /**
     * Measurement name -> aggregated measurement
     * Measurements per position
     */
    Map<String, Aggregation>
>

data class AggregatedBatch(
    val timestampNanos: Long,
    val aggregationWidth: Duration,
    val metric: String,
    val positions: MetricPositions,
)

private fun epochTime(epochMillis: Long): TimeMark {
    return object : TimeMark {
        override fun elapsedNow(): Duration {
            return (System.currentTimeMillis() - epochMillis).milliseconds
        }
    }
}

private fun timeColumnMillis(divisor: Duration): Long {
    val now = System.currentTimeMillis()
    return now - (now % divisor.inWholeMilliseconds)
}

class Aggregator(
    private val aggregationWidth: Duration = 10.seconds,
    private val delay_fn: suspend (duration: Duration) -> Unit = ::delay
) : MetricsPipeline<AggregatedBatch>, MetricsSink {
    @Volatile
    private var currentBatch = MetricsMap()
    private var lastEmit: Long = timeColumnMillis(aggregationWidth)

    override fun consume(): Flow<AggregatedBatch> {
        return flow {
            while (true) {
                val nextEmit = epochTime(lastEmit) + aggregationWidth
                val timeToNextEmit = nextEmit.elapsedNow()
                lastEmit += aggregationWidth.inWholeMilliseconds
                if (0.seconds < timeToNextEmit || aggregationWidth < -timeToNextEmit) {
                    // Skip a time column because of sadness.
                    // Resume on the column cadence as best we can.
                    yield()
                    continue
                }
                delay_fn(-timeToNextEmit)

                val batch = currentBatch
                currentBatch = MetricsMap()

                for ((metric, positions) in batch) {
                    emit(
                        AggregatedBatch(
                            timestampNanos = lastEmit * 1000000,
                            aggregationWidth = aggregationWidth,
                            metric = metric,
                            positions = positions,
                        )
                    )
                }
            }
        }
    }

    override fun emit(metrics: Metrics) {
        val position = metrics.dimensionPosition()

        val metricPositions = currentBatch.getOrPut(metrics.name, ::DimensionPositionMap)

        // Simple measurements are statistic_sets
        for ((name, value) in metrics.metricMeasurements) {
            val aggregation = metricPositions
                .getOrPut(position, ::AggregationMap)
                .getOrPut(name, Aggregation::StatisticSet)
            when (aggregation) {
                is Aggregation.StatisticSet -> {
                    aggregation.accumulate(value)
                }
                is Aggregation.Histogram -> {
                    // TODO: logging
                }
            }
        }

        // Distributions are histograms
        for ((name, value) in metrics.metricDistributions) {
            val aggregation = metricPositions
                .getOrPut(position, ::AggregationMap)
                .getOrPut(name, Aggregation::Histogram)
            when (aggregation) {
                is Aggregation.StatisticSet -> {
                    // TODO: Logging
                }
                is Aggregation.Histogram -> {
                    aggregation.accumulate(value)
                }
            }
        }
    }
}

typealias DimensionPosition = Set<Metrics.Dimension>

typealias AggregationMap = ConcurrentHashMap<String, Aggregation>
typealias DimensionPositionMap = ConcurrentHashMap<DimensionPosition, AggregationMap>
typealias MetricsMap = ConcurrentHashMap<String, DimensionPositionMap>

fun Metrics.dimensionPosition(): DimensionPosition {
    return metricDimensions
        .asSequence()
        .map { entry -> entry.value }
        .toSet()
}

/**
 * Base 10 2-significant-figures bucketing
 */
fun bucket(value: Long): Long {
    if (value < 100L) return max(0, value)
    val power = log(value.toDouble(), 10.0)
    val effectivePower = max(0, (power - 1).toInt())
    val trashColumn = 10.0.pow(effectivePower).toLong()
    val trash = value % trashColumn
    return if (trash < 1) {
        value
    } else {
        value + trashColumn - trash
    }
}

fun bucketBelow(valueIn: Long): Long {
    val value = valueIn - 1
    if (value < 100L) return max(0, value)
    val power = log(value.toDouble(), 10.0)
    val effectivePower = max(0, (power - 0.00001 - 1).toInt())
    val trashColumn = 10.0.pow(effectivePower).toLong()
    val trash = value % trashColumn
    return value - trash
}

/**
 * Base 2 bucketing. This is plain bucketing; no sub-steps, just the next highest base2 power of value.
 */
fun bucketBase2(value: Long): Long {
    val power = ceil(log(value.toDouble(), 2.0))
    return 2.0.pow(power).toLong()
}

sealed interface Aggregation {
    data class Histogram(
        val bucketCounts: ConcurrentHashMap<Long, LongAdder> = ConcurrentHashMap(),
    ) : Aggregation {
        fun accumulate(value: Long) {
            bucketCounts.getOrPut(bucket(value), ::LongAdder).increment()
        }
    }

    data class StatisticSet(
        val min: DoubleAccumulator = DoubleAccumulator(Math::min, Double.MAX_VALUE),
        val max: DoubleAccumulator = DoubleAccumulator(Math::max, Double.MIN_VALUE),
        val sum: DoubleAdder = DoubleAdder(),
        val count: LongAdder = LongAdder(),
    ) : Aggregation {
        fun accumulate(value: Number) {
            val v = value.toDouble()
            min.accumulate(v)
            max.accumulate(v)
            sum.add(v)
            count.add(1)
        }
    }
}
