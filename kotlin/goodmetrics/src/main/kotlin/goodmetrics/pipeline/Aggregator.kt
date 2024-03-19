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
import kotlin.math.E
import kotlin.math.absoluteValue
import kotlin.math.ceil
import kotlin.math.exp
import kotlin.math.floor
import kotlin.math.log
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.sign
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

/**
 * When measuring distribution metrics, how do you want your metrics to be expressed?
 */
sealed interface DistributionMode {
    fun getAggregations(dimensionPositionMap: DimensionPositionMap, position: DimensionPosition, name: String): Aggregation
    object Histogram : DistributionMode {
        override fun getAggregations(dimensionPositionMap: DimensionPositionMap, position: DimensionPosition, name: String): Aggregation {
           return dimensionPositionMap
               .getOrPut(position, ::AggregationMap)
               .getOrPut(name, Aggregation::Histogram)
        }
    }
    data class ExponentialHistogram(val desiredScale: Int) : DistributionMode {
        override fun getAggregations(dimensionPositionMap: DimensionPositionMap, position: DimensionPosition, name: String): Aggregation {
            return dimensionPositionMap
                .getOrPut(position, ::AggregationMap)
                .getOrPut(name) { Aggregation.ExponentialHistogram(desiredScale.toUInt()) }
        }
    }
}

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
    private val delay_fn: suspend (duration: Duration) -> Unit = ::delay,
    val distributionMode: DistributionMode
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
                is Aggregation.ExponentialHistogram -> {
                    // TODO: logging
                }
            }
        }

        for ((name, value) in metrics.metricDistributions) {
            when (val aggregation = this.distributionMode.getAggregations(metricPositions, position, name)) {
                is Aggregation.StatisticSet -> {
                    // TODO: Logging
                }
                is Aggregation.Histogram -> {
                    aggregation.accumulate(value)
                }
                is Aggregation.ExponentialHistogram -> {
                    aggregation.accumulate(value.toDouble())
                }
            }
        }
    }

    override fun close() {
        // nothing to do here now
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

// log_2(e). This is the most precision the JVM will give us
const val LOG2_E = 1.4426950408889634;

// ln_2. This is the most precision the JVM will give us
const val LN_2 = 0.6931471805599453;

sealed interface Aggregation {
    data class ExponentialHistogram(
        /**
         * Desired scale will drop as necessary to match the static max buckets configuration.
         * This will happen dynamically in response to observed range. If your distribution
         * range falls within 160 contiguous buckets somewhere the desired scale's range, then
         * your output scale will match your desired scale. If your observed range exceeds 160
         * buckets then scale will be reduced to reflect the data's width.
         */
        var actualScale: UInt,
        val maxBucketCount: UInt = 160u,
        var bucketStartOffset: UInt = 0u,
        val positiveBuckets: ArrayDeque<Long> = ArrayDeque(),
        val negativeBuckets: ArrayDeque<Long> = ArrayDeque()
    ) : Aggregation {

        fun accumulate(value: Double) {
            accumulateCount(value, 1)
        }

        private fun accumulateCount(value: Double, count: Long) {
            // This may be before or after the current range, and that range might need to be expanded.
            val scaleIndex = mapValueToScaleIndex(actualScale.toInt(), value)

            // Initialize the histogram to center on the first data point. That should probabilistically
            // reduce the amount of shifting we do over time, for normal distributions.
            if (isEmpty()) {
                bucketStartOffset = scaleIndex - maxBucketCount / 2u
            }

            var localIndex = scaleIndex.toInt() - bucketStartOffset.toInt()

            while (localIndex < 0 && rotateRangeDownOneIndex()) {
                localIndex++
            }

            while (maxBucketCount.toInt() <= localIndex && rotateRangeUpOneIndex()) {
                localIndex--
            }

            if (localIndex < 0 || maxBucketCount.toInt() <= localIndex) {
                if (zoomOut()) {
                    accumulateCount(value, count)
                    return
                }
                // if we didn't zoom out then we're at the end of the range.
                localIndex = maxBucketCount.toInt() - 1
            }

            val index = min(maxBucketCount - 1u, localIndex.toUInt())
            val buckets = getMutableBucketsForValue(value)
            val size = max(0, (index.toInt() - buckets.size)+ 1)
            buckets.addAll(List(size) { 0 })
            buckets[index.toInt()] += count
        }

        private fun zoomOut(): Boolean {
            if (actualScale == 0u) {
                return false
            }
            val oldScale = actualScale.toInt()
            val oldBucketStartOffset = bucketStartOffset
            val oldPositives = takePositives()
            val oldNegatives = takeNegatives()

            actualScale--
            bucketStartOffset = 0u

            // now just re-ingest
            for ((oldIndex, count) in oldPositives.withIndex()) {
                if (0 < count) {
                    val value = lowerBoundary(oldScale, (oldBucketStartOffset + oldIndex.toUInt()))
                    accumulateCount(value, count)
                }
            }
            for ((oldIndex, count) in oldNegatives.withIndex()) {
                if (0 < count) {
                    val value = -lowerBoundary(oldScale, (oldBucketStartOffset + oldIndex.toUInt()))
                    accumulateCount(value, count)
                }
            }

            return true
        }

        private fun rotateRangeDownOneIndex(): Boolean {
            if (positiveBuckets.size < maxBucketCount.toInt() && negativeBuckets.size < maxBucketCount.toInt()) {
                if (!positiveBuckets.isEmpty()) {
                    positiveBuckets.addFirst(0)
                }
                if (!negativeBuckets.isEmpty()) {
                    negativeBuckets.addFirst(0)
                }
                bucketStartOffset--
                return true
            }
            return false
        }

        private fun rotateRangeUpOneIndex(): Boolean {
            if ((positiveBuckets.firstOrNull() ?: 0L) == 0L && (negativeBuckets.firstOrNull() ?: 0L) == 0L) {
                positiveBuckets.removeFirstOrNull()
                negativeBuckets.removeFirstOrNull()
                bucketStartOffset++
                return true
            }
            return false
        }

        private fun getMutableBucketsForValue(value: Double): ArrayDeque<Long> {
            return if (value.sign == 1.0) {
                positiveBuckets
            } else {
                negativeBuckets
            }
        }

        private fun isEmpty(): Boolean = positiveBuckets.isEmpty() && negativeBuckets.isEmpty()

        fun count(): Long = positiveBuckets.sum() + negativeBuckets.sum()

        /**
         * This is an approximation, just using the positive buckets for the sum.
         */
        fun sum(): Double = positiveBuckets.mapIndexed { index, count -> lowerBoundary(actualScale.toInt(), index.toUInt()) * count }.sum()

        /**
         * This is an approximation, just using the positive buckets for the min.
         */
        fun min(): Double {
            return positiveBuckets.withIndex().firstOrNull { 0 < it.value }?.let { lowerBoundary(actualScale.toInt(), it.index.toUInt()) } ?: 0.0
        }

        fun max(): Double {
            return positiveBuckets.withIndex().lastOrNull { 0 < it.value }?.let { lowerBoundary(actualScale.toInt(), it.index.toUInt()) } ?: 0.0
        }

        fun scale(): Int = actualScale.toInt()

        fun bucketStartOffset(): Int = bucketStartOffset.toInt()

        fun takePositives(): ArrayDeque<Long> {
            val positives = ArrayDeque(positiveBuckets)
            this.positiveBuckets.clear()
            return positives
        }

        fun takeNegatives(): ArrayDeque<Long> {
            val negatives = ArrayDeque(negativeBuckets)
            this.negativeBuckets.clear()
            return negatives
        }

        fun hasNegatives(): Boolean = this.negativeBuckets.isNotEmpty()

        fun valueCounts(): Sequence<Pair<Double, Long>> {
            return this.negativeBuckets.mapIndexed { index, count ->
                Pair(
                    lowerBoundary(actualScale.toInt(), bucketStartOffset + index.toUInt()),
                    count
                )
            }.asSequence() + this.positiveBuckets.mapIndexed { index, count ->
                Pair(
                    lowerBoundary(actualScale.toInt(), bucketStartOffset + index.toUInt()),
                    count
                )
            }.asSequence()
        }
    }
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

/**
 * treats negative numbers as positive - you gotta accumulate into a negative array
 */
fun mapValueToScaleIndex(scale: Int, rawValue: Number): UInt {
    val value = rawValue.toDouble().absoluteValue
    val scaleFactor = LOG2_E * 2.0.pow(scale)
    return floor(log(value, E) * scaleFactor).toUInt()
}

/**
 * obviously only supports positive indices. If you want a negative boundary, flip the sign on the return value.
 * per the wonkadoo instructions found at: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram
 *   > The positive and negative ranges of the histogram are expressed separately. Negative values are mapped by
 *   > their absolute value into the negative range using the same scale as the positive range. Note that in the
 *   > negative range, therefore, histogram buckets use lower-inclusive boundaries.
 */
fun lowerBoundary(scale: Number, index: UInt): Double {
    val inverseScaleFactor = LN_2 * 2.0.pow(-scale.toInt())
    return exp(index.toDouble() * inverseScaleFactor)
}
