package goodmetrics.pipeline

import goodmetrics.Metrics
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow

/**
 * Threadsafe pipeline starter
 */
class SynchronizingBuffer(
    maxQueuedItems: Int = 1024
) : MetricsPipeline<Metrics>, MetricsSink {
    private val metricsQueue: Channel<Metrics> = Channel(
        capacity = maxQueuedItems,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
        onUndeliveredElement = this::failedToDeliver
    )

    override fun consume(): Flow<Metrics> {
        return metricsQueue.consumeAsFlow()
    }

    override fun emit(metrics: Metrics) {
        metricsQueue.trySend(metrics)
    }

    override fun close() {
        metricsQueue.close()
    }

    private fun failedToDeliver(_metrics: Metrics) {
        // TODO: record metrics
    }
}
