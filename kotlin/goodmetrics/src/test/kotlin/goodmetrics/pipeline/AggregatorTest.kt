package goodmetrics.pipeline

import goodmetrics.Metrics
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

internal class AggregatorTest {
    lateinit var gotBatch: Mutex
    lateinit var aggregatorSleep: Mutex

    val batches = mutableListOf<AggregatedBatch>()

    fun endAggregatorSleep() {
        assertTrue(aggregatorSleep.isLocked)
        aggregatorSleep.unlock()
    }

    suspend fun sleepAggregator() {
        aggregatorSleep.lock()
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

    private fun testOneWindow(runWindow: (Aggregator) -> Unit): AggregatedBatch {
        return runBlocking {
            val aggregator = Aggregator(aggregationWidth = 1.seconds, delay_fn = ::oneSecondDelay)
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
}
