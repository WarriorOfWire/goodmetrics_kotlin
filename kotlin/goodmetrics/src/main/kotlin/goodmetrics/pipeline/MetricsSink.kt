package goodmetrics.pipeline

import goodmetrics.Metrics

interface MetricsSink : AutoCloseable {
    fun emit(metrics: Metrics)
}
