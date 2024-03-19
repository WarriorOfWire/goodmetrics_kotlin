package goodmetrics.pipeline

import goodmetrics.Metrics
import kotlin.math.absoluteValue
import kotlin.random.Random
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll

internal class AggregatorTest {
    lateinit var gotBatch: Mutex
    lateinit var aggregatorSleep: Mutex

    private val epsilon = 1.0 / 128.0
    val batches = mutableListOf<AggregatedBatch>()

    fun endAggregatorSleep() {
        assertTrue(aggregatorSleep.isLocked)
        aggregatorSleep.unlock()
    }

    suspend fun sleepAggregator() {
        aggregatorSleep.lock()
    }

    private fun assertEqualsEpsilon(j: Double, k: Double, message: String) {
        val difference = (j - k).absoluteValue
        assertTrue(difference < epsilon, "$message: $j != $k with epsilon $epsilon")
    }

    private fun assertValueLowerBoundary(exponentialHistogram: Aggregation.ExponentialHistogram, value: Number, expectedLowerBoundary: Number) {
        val observedIndex = mapValueToScaleIndex(exponentialHistogram.scale(), value.toDouble())
        val observedBoundary = lowerBoundary(exponentialHistogram.scale(), observedIndex)
        assertEqualsEpsilon(expectedLowerBoundary.toDouble(), observedBoundary, "boundary matches")
    }

    @BeforeTest
    fun reset() {
        aggregatorSleep = Mutex(true)
        gotBatch = Mutex(true)
        batches.clear()
    }

    private suspend fun oneSecondDelay(delay: Duration) {
        assertTrue(delay <= 1.seconds, "delay: $delay")
        sleepAggregator()
    }

    fun metrics(distribution: Long? = null): Metrics {
        val m = Metrics("test", 123, 456)
        m.dimension("tes", "t")
        m.measure("f", 5)
        if (distribution != null) {
            m.distribution("distribution", distribution)
        }
        return m
    }

    private fun testOneWindow(distributionMode: DistributionMode = DistributionMode.Histogram, runWindow: (Aggregator) -> Unit): AggregatedBatch {
        return runBlocking {
            val aggregator = Aggregator(aggregationWidth = 1.seconds, delay_fn = ::oneSecondDelay, distributionMode = distributionMode)
            val collectorJob = launch {
                aggregator.consume().collect { batch ->
                    batches.add(batch)
                    gotBatch.unlock()
                }
            }

            runWindow(aggregator)

            endAggregatorSleep()
            gotBatch.lock()

            assertEquals(1, batches.size)
            val batch = batches[0]
            assertEquals("test", batch.metric)
            collectorJob.cancel()

            batch
        }
    }

    @Test
    fun testDistribution() {
        val batch = testOneWindow { aggregator ->
            (listOf<Long>(1888, 1809, 1818, 2121, 2220) + (1..995).map { 1888L }).forEach { i ->
                aggregator.emit(metrics(distribution = i))
            }
        }

        val aggregations = batch.positions[setOf<Metrics.Dimension>(Metrics.Dimension.String("tes", "t"))]!!

        assertEquals(setOf("f", "distribution"), aggregations.keys)
        val aggregation = aggregations["distribution"]!!
        assertTrue(aggregation is Aggregation.Histogram)
        assertEquals(998, aggregation.bucketCounts[1900]!!.toLong())
        assertEquals(1, aggregation.bucketCounts[2200]!!.toLong())
        assertEquals(1, aggregation.bucketCounts[2300]!!.toLong())
    }

    @Test
    fun testStatisticSet() {
        val batch = testOneWindow { aggregator ->
            aggregator.emit(metrics())
            aggregator.emit(metrics())
        }

        assertEquals(setOf(setOf<Metrics.Dimension>(Metrics.Dimension.String("tes", "t"))), batch.positions.keys)
        val aggregations = batch.positions[setOf<Metrics.Dimension>(Metrics.Dimension.String("tes", "t"))]!!

        assertEquals(setOf("f"), aggregations.keys)
        val aggregation = aggregations["f"]!!
        assertTrue(aggregation is Aggregation.StatisticSet)
        assertEquals(5.0, aggregation.min.get())
        assertEquals(5.0, aggregation.max.get())
        assertEquals(10.0, aggregation.sum.sum())
        assertEquals(2L, aggregation.count.sum())
    }

    @Test
    fun emit() {
    }



    @Test
    fun testMapValueToScaleIndex() {
        assertEquals(1275u, mapValueToScaleIndex(6, 1_000_000))
        assertEquals(1275u + 160u, mapValueToScaleIndex(6, 5_650_000))

        assertEquals(637u, mapValueToScaleIndex(5, 1_000_000))
        assertEquals(637u + 160u, mapValueToScaleIndex(5, 32_000_000))
    }

    @Test
    fun testIndicesScaleZeroPositiveNumbers() {
        val e = Aggregation.ExponentialHistogram(0u)
        assertEquals(0u, mapValueToScaleIndex(e.scale(), 0))
        assertValueLowerBoundary(e, 0, 1)
        assertValueLowerBoundary(e, 1, 1)
        assertValueLowerBoundary(e, 2, 2)
        assertValueLowerBoundary(e, 3, 2)
        assertValueLowerBoundary(e, 4, 4)
        assertValueLowerBoundary(e, 7, 4)
        assertValueLowerBoundary(e, 8, 4)
        assertValueLowerBoundary(e, 8.1, 8)
    }

    @Test
    fun testIndicesScaleZeroNegativeNumbers() {
        val e = Aggregation.ExponentialHistogram(0u)
        assertEquals(0u, mapValueToScaleIndex(e.scale(), 0))
        assertValueLowerBoundary(e, -0, 1)
        assertValueLowerBoundary(e, -1, 1)
        assertValueLowerBoundary(e, -2, 2)
        assertValueLowerBoundary(e, -3, 2)
        assertValueLowerBoundary(e, -4, 4)
        assertValueLowerBoundary(e, -7, 4)
        assertValueLowerBoundary(e, -8, 4)
        assertValueLowerBoundary(e, -8.1, 8)
    }

    @Test
    fun testIndicesScaleOnePositiveNumbers() {
        val e = Aggregation.ExponentialHistogram(1u)
        assertEquals(0u, mapValueToScaleIndex(e.scale(), 0))
        assertValueLowerBoundary(e, 0, 1)
        assertValueLowerBoundary(e, 1, 1)
        assertValueLowerBoundary(e, 2, 2)
        assertValueLowerBoundary(e, 3, 2.828)
        assertValueLowerBoundary(e, 4, 4)
        assertValueLowerBoundary(e, 7, 5.657)
        assertValueLowerBoundary(e, 8, 5.657)
        assertValueLowerBoundary(e, 8.1, 8)
    }

    @Test
    fun testIndicesScaleTwoPositiveNumbers() {
        val e = Aggregation.ExponentialHistogram(2u)
        assertEquals(0u, mapValueToScaleIndex(e.scale(), 0))
        assertValueLowerBoundary(e, 0, 1)
        assertValueLowerBoundary(e, 1, 1)
        assertValueLowerBoundary(e, 2, 2)
        assertValueLowerBoundary(e, 3, 2.828)
        assertValueLowerBoundary(e, 4, 4)
        assertValueLowerBoundary(e, 7, 6.727)
        assertValueLowerBoundary(e, 8, 6.727)
        assertValueLowerBoundary(e, 8.1, 8)
    }

    @Test
    fun testIndicesScaleThreePositiveNumbers() {
        val e = Aggregation.ExponentialHistogram(3u)
        assertEquals(0u, mapValueToScaleIndex(e.scale(), 0))
        assertValueLowerBoundary(e, 0, 1)
        assertValueLowerBoundary(e, 1, 1)
        assertValueLowerBoundary(e, 2, 2)
        assertValueLowerBoundary(e, 3, 2.828)
        assertValueLowerBoundary(e, 4, 4)
        assertValueLowerBoundary(e, 7, 6.727)
        assertValueLowerBoundary(e, 8, 7.337)
        assertValueLowerBoundary(e, 8.1, 8)
    }

    @Test
    fun testIndicesScaleFourPositiveNumbers() {
        val e = Aggregation.ExponentialHistogram(4u)
        assertEquals(0u, mapValueToScaleIndex(e.scale(), 0.0))
        assertValueLowerBoundary(e, 0, 1)
        assertValueLowerBoundary(e, 1, 1)
        assertValueLowerBoundary(e, 2, 2)
        assertValueLowerBoundary(e, 3, 2.954)
        assertValueLowerBoundary(e, 4, 4)
        assertValueLowerBoundary(e, 5, 4.967)
        assertValueLowerBoundary(e, 6, 5.907)
        assertValueLowerBoundary(e, 7, 6.727)
        assertValueLowerBoundary(e, 8, 7.661)
        assertValueLowerBoundary(e, 8.1, 8)
    }

    @Test
    fun testIndicesScaleDowngradePositiveNumbers() {
        val e = Aggregation.ExponentialHistogram(8u)
        e.accumulate(24_000_000.0)
        assertEquals(8, e.scale(), "initial value should not change scale since it falls in the numeric range")
        assertEquals(81, e.positiveBuckets.size, "initial value should be in the middle")
        assertEquals(1, e.positiveBuckets[80], "initial value should go in index 80 because that is halfway to 160")
        assertEquals(6196, e.bucketStartOffset(), "bucket start offset should index into scale 8")

        // assert some bucket boundaries for convenience
        assertValueLowerBoundary(e, 24_000_000, 23984931.775)
        assertValueLowerBoundary(e, 24_040_000, 23984931.775)
        assertValueLowerBoundary(e, 24_050_000, 24049961.522)

        assertEqualsEpsilon(19313750.368, lowerBoundary(8, 6196u), "lower boundary of histogram")
        assertEqualsEpsilon(29785874.896, lowerBoundary(8, 6196u + 160u), "upper boundary of histogram")

        // Accumulate some data in a bucket's range
        for (i in 0 until 40_000) {
            e.accumulate((24_000_000 + i).toDouble())
        }

        assertEquals(40_001, e.positiveBuckets[80], "initial value should go in index 80 because that is halfway to 160")

        e.accumulate(24_050_000.0)
        assertEquals(8, e.scale(), "a value in the next higher bucket should not change the scale")
        assertEquals(82, e.positiveBuckets.size, "bucket count should be able to increase densely when a new bucket value is observed")
        assertEquals(1, e.positiveBuckets[81], "index 81 has a new count")
        assertEquals(6196, e.bucketStartOffset(), "bucket start offset does not change when adding a bucket in the same range")

        // Poke at growth boundary conditions
        e.accumulate(23_984_000.0)
        assertEquals(8, e.scale(), "a value in the next lower bucket should not change the scale")
        assertEquals(82, e.positiveBuckets.size, "bucket count should not increase when a new bucket value is observed within the covered range")
        assertEquals(1, e.positiveBuckets[79], "index 79 has a new count")
        assertEquals(6196, e.bucketStartOffset(), "bucket start offset does not change when using a bucket in the same range")

        e.accumulate(19_313_750.0)
        assertEquals(8, e.scale(), "a value below the covered range should not change the scale yet because there is room above the observed range to shift")
        assertEquals(83, e.positiveBuckets.size, "bucket count should not increase when a new bucket value is observed within the covered range")
        assertEquals(1, e.positiveBuckets[0], "index 0 has a new count")
        assertEquals(6195, e.bucketStartOffset(), "bucket start offset changes because we rotated down 1 position")
        assertEqualsEpsilon(29705335.561, lowerBoundary(8, 6195u + 160u), "new upper boundary of histogram")

        //
        // -------- Expand histogram range with a big number --------
        //
        e.accumulate(29_705_336.0)
        assertEquals(3177u, mapValueToScaleIndex(7, 29_705_336.0), "this value pushes the length of scale 7 also")
        assertEquals(7, e.scale(), "a value above the covered range should now change the scale because the lower end is populated while the upper end is beyond the range this scale can cover in 160 buckets")
        assertEquals(160, e.positiveBuckets.size, "bucket count should be sensible after rescale")
        assertEquals(1, e.positiveBuckets[e.positiveBuckets.size - 1], "last index has a new count")
        assertEquals(3018, e.bucketStartOffset(), "bucket start offset changes because we scaled and rotated")

        //
        // -------- Skip several zoom scale steps in a single accumulate --------
        //

        val recursiveScaleStartCount = e.count()
        assertEquals(2199023255551.996, lowerBoundary(2, 164u), "this value gets us down into scale 2")
        assertEqualsEpsilon(4.0, lowerBoundary(2, 8u), "this value gets us down into scale 2")
        assertEqualsEpsilon(4.757, lowerBoundary(2, 9u), "this value gets us down into scale 2")
        // pin the bucket's low value, at scale 2's index 8. It's not in scale 2 yet though!
        e.accumulate(4.25)
        // now blow the range wide, way past scale 7, resulting in a recursive scale down from 7 to precision 2.
        e.accumulate(2_199_023_255_552.0)
        assertEquals(2, e.scale(), "this value range should force scale range 2")
        assertEquals(8, e.bucketStartOffset(), "bucket start offset should match the first element, since we rotated and grew out to the larger value")
        assertEquals(1, e.positiveBuckets[8 - 8], "this is the 4.0 bucket, and 4.25 should go in it.")
        assertEquals(1, e.positiveBuckets[164 - 8], "this is the bucket for the big numer.")
        assertEquals(recursiveScaleStartCount + 2, e.count(), "2 more reports were made. The histogram maintains every count across rescaling, even recursive rescaling")
    }

    // Helps us look for random index crashes
    @Test
    fun testExponentialHistogramFuzz() {
        val start = System.nanoTime()
        while ((System.nanoTime() - start).milliseconds < 50.milliseconds) {
            val e = Aggregation.ExponentialHistogram(8u)
            val innerStart = System.nanoTime()
            while ((System.nanoTime() - innerStart).milliseconds < 1.milliseconds) {
                e.accumulate(1_000_000_000_000_000.0 * Random.nextDouble())
            }
        }
    }

    // Helps us look for random index crashes
    @Test
    fun testExponentialHistogramFuzzNegative() {
        val start = System.nanoTime()
        while ((System.nanoTime() - start).milliseconds < 50.milliseconds) {
            val e = Aggregation.ExponentialHistogram(8u)
            val innerStart = System.nanoTime()
            while ((System.nanoTime() - innerStart).milliseconds < 1.milliseconds) {
                e.accumulate(-1_000_000_000_000_000.0 * Random.nextDouble())
            }
        }
    }

    // Verify multi-threaded access is indeed safe
    @Test
    fun testExponentialHistogramMultiThreadedAccess() = runBlocking {
        val e = Aggregation.ExponentialHistogram(0u)
        listOf(
            // coroutine 1
            launch {
                // Accumulate at the same time as 2
                e.accumulate(1.0)
                // coroutine 2 should be accumulating
                assertEquals(1, e.count())
                // Wait
                delay(5.milliseconds)
                e.accumulate(50_000_000.0)
                // Wait while coroutine 2 does more aggregations
                delay(5.milliseconds)
                // Both should be asserting the same thing is true
                assertEquals(5, e.count())
            },
            // coroutine 2
            launch {
                // Accumulate at the same time as 1
                e.accumulate(2.0)
                assertEquals(2, e.count())
                // We've accumulated at the same time, so we should only have 2 counts
                assertTrue(e.count() <= 2)
                // Wait
                delay(5.milliseconds)
                // Coroutine 1 should have accumulated again, so we should have 3 counts
                assertTrue(e.count() >= 3)
                // Now accumulate some more
                e.accumulate(5.0)
                e.accumulate(42_000_000.0)
                delay(2.milliseconds)
                // Both should be asserting the same thing is true
                assertEquals(5, e.count())
            }
        ).joinAll()

    }
}
