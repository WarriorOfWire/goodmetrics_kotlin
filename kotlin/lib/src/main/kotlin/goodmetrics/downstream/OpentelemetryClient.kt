package goodmetrics.downstream

import goodmetrics.Metrics
import goodmetrics.io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpcKt
import goodmetrics.io.opentelemetry.proto.collector.metrics.v1.exportMetricsServiceRequest
import goodmetrics.io.opentelemetry.proto.common.v1.KeyValue
import goodmetrics.io.opentelemetry.proto.common.v1.anyValue
import goodmetrics.io.opentelemetry.proto.common.v1.instrumentationScope
import goodmetrics.io.opentelemetry.proto.common.v1.keyValue
import goodmetrics.io.opentelemetry.proto.metrics.v1.AggregationTemporality
import goodmetrics.io.opentelemetry.proto.metrics.v1.Metric
import goodmetrics.io.opentelemetry.proto.metrics.v1.ResourceMetrics
import goodmetrics.io.opentelemetry.proto.metrics.v1.ScopeMetrics
import goodmetrics.io.opentelemetry.proto.metrics.v1.gauge
import goodmetrics.io.opentelemetry.proto.metrics.v1.histogram
import goodmetrics.io.opentelemetry.proto.metrics.v1.histogramDataPoint
import goodmetrics.io.opentelemetry.proto.metrics.v1.metric
import goodmetrics.io.opentelemetry.proto.metrics.v1.numberDataPoint
import goodmetrics.io.opentelemetry.proto.metrics.v1.resourceMetrics
import goodmetrics.io.opentelemetry.proto.metrics.v1.scopeMetrics
import goodmetrics.io.opentelemetry.proto.metrics.v1.sum
import goodmetrics.io.opentelemetry.proto.resource.v1.resource
import goodmetrics.pipeline.AggregatedBatch
import goodmetrics.pipeline.Aggregation
import goodmetrics.pipeline.bucket
import io.grpc.CallOptions
import io.grpc.ClientInterceptor
import io.grpc.Deadline
import io.grpc.ManagedChannel
import io.grpc.netty.GrpcSslContexts
import io.grpc.netty.NettyChannelBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

sealed interface PrescientDimensions {
    /**
     * Include resource dimensions on the OTLP resource.
     */
    data class AsResource(val resourceDimensions: Map<String, Metrics.Dimension>) : PrescientDimensions

    /**
     * Include resource dimensions on each metric instead of on the Resource. You'd use this for
     * downstreams that either do not support or do something undesirable with Resource dimensions.
     */
    data class AsDimensions(val sharedDimensions: Map<String, Metrics.Dimension>) : PrescientDimensions
}

enum class SecurityMode {
    Plaintext,
    Insecure,
    Tls,
}

/**
 * This client should be used as a last resort, in defeat, if you
 * cannot use the goodmetrics protocol. Opentelemetry is highly
 * lossy and inflexible. I'm doing my best here, but you're not
 * getting the full goodmetrics experience if you're still
 * addicted to opentelemetry line protocol.
 */
class OpentelemetryClient(
    private val channel: ManagedChannel,
    private val prescientDimensions: PrescientDimensions,
    private val timeout: Duration,
) {
    companion object {
        fun connect(
            sillyOtlpHostname: String = "localhost",
            port: Int = 5001,
            prescientDimensions: PrescientDimensions,
            securityMode: SecurityMode,
            /**
             * stuff like MetadataUtils.newAttachHeadersInterceptor()
             */
            interceptors: List<ClientInterceptor>,
            timeout: Duration = 5.seconds,
        ): OpentelemetryClient {
            val channelBuilder = NettyChannelBuilder.forAddress(sillyOtlpHostname, port)
            when (securityMode) {
                SecurityMode.Tls -> {
                    channelBuilder.useTransportSecurity()
                }
                SecurityMode.Insecure -> {
                    channelBuilder.useTransportSecurity()
                    channelBuilder.sslContext(
                        GrpcSslContexts.forClient()
                            .trustManager(InsecureTrustManagerFactory.INSTANCE)
                            .build()
                    )
                }
                SecurityMode.Plaintext -> {
                    channelBuilder.usePlaintext()
                }
            }
            channelBuilder.intercept(interceptors)
            return OpentelemetryClient(channelBuilder.build(), prescientDimensions, timeout)
        }
    }
    private fun stub(): MetricsServiceGrpcKt.MetricsServiceCoroutineStub = MetricsServiceGrpcKt.MetricsServiceCoroutineStub(
        channel,
        CallOptions.DEFAULT
            .withDeadline(Deadline.after(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS))
    )

    suspend fun sendMetricsBatch(batch: List<Metrics>) {
        val resourceMetricsBatch = asResourceMetrics(batch)
        stub().export(
            exportMetricsServiceRequest {
                resourceMetrics.add(resourceMetricsBatch)
            }
        )
    }

    suspend fun sendPreaggregatedBatch(batch: List<AggregatedBatch>) {
        val resourceMetricsBatch = asResourceMetricsFromBatch(batch)
        stub().export(
            exportMetricsServiceRequest {
                resourceMetrics.add(resourceMetricsBatch)
            }
        )
    }

    private fun asResourceMetricsFromBatch(batch: List<AggregatedBatch>): ResourceMetrics {
        return resourceMetrics {
            prescientResource?.let { this.resource = it }
            for (aggregate in batch) {
                this.scopeMetrics.add(aggregate.asOtlpScopeMetrics())
            }
        }
    }

    private fun AggregatedBatch.asOtlpScopeMetrics(): ScopeMetrics = scopeMetrics {
        scope = library
        metrics.addAll(this@asOtlpScopeMetrics.asGoofyOtlpMetricSequence().asIterable())
    }

    private fun asResourceMetrics(batch: List<Metrics>): ResourceMetrics = resourceMetrics {
        prescientResource?.let { this.resource = it }
        for (metric in batch) {
            this.scopeMetrics.add(asScopeMetrics(batch))
        }
    }

    private fun asScopeMetrics(batch: List<Metrics>): ScopeMetrics = scopeMetrics {
        scope = library
        metrics.addAll(batch.asSequence().flatMap { it.asGoofyOtlpMetricSequence() }.asIterable())
    }

    private fun AggregatedBatch.asGoofyOtlpMetricSequence(): Sequence<Metric> = sequence {
        for ((position, measurements) in this@asGoofyOtlpMetricSequence.positions) {
            val otlpDimensions = position.map { it.asOtlpKeyValue() }
            for ((measurementName, aggregation) in measurements) {
                when (aggregation) {
                    is Aggregation.Histogram -> {
                        yield(
                            metric {
                                name = "${this@asGoofyOtlpMetricSequence.metric}_$measurementName"
                                unit = "1"
                                histogram = aggregation.asOtlpHistogram(otlpDimensions, this@asGoofyOtlpMetricSequence.timestampNanos, aggregationWidth)
                            }
                        )
                    }
                    is Aggregation.StatisticSet -> {
                        yieldAll(aggregation.statisticSetToOtlp(this@asGoofyOtlpMetricSequence.metric, measurementName, timestampNanos, aggregationWidth, otlpDimensions))
                    }
                }
            }
        }
    }

    private fun Aggregation.StatisticSet.statisticSetToOtlp(
        metric: String,
        measurementName: String,
        timestampNanos: Long,
        aggregationWidth: Duration,
        dimensions: Iterable<KeyValue>,
    ): Sequence<Metric> = sequence {
        yield(statisticSetDataPoint(metric, measurementName, "min", min, timestampNanos, aggregationWidth, dimensions))
        yield(statisticSetDataPoint(metric, measurementName, "max", max, timestampNanos, aggregationWidth, dimensions))
        yield(statisticSetDataPoint(metric, measurementName, "sum", sum, timestampNanos, aggregationWidth, dimensions))
        yield(statisticSetDataPoint(metric, measurementName, "count", count, timestampNanos, aggregationWidth, dimensions))
    }

    private fun statisticSetDataPoint(
        metricName: String,
        measurementName: String,
        statisticSetComponent: String,
        value: Number,
        timestampNanos: Long,
        aggregationWidth: Duration,
        dimensions: Iterable<KeyValue>,
    ): Metric = metric {
        name = "${metricName}_${measurementName}_$statisticSetComponent"
        unit = "1"
        sum = sum {
            isMonotonic = false
            // because cumulative is bullshit
            aggregationTemporality = AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA
            dataPoints.add(newNumberDataPoint(value, timestampNanos, aggregationWidth, dimensions))
        }
    }

    private fun Metrics.asGoofyOtlpMetricSequence(): Sequence<Metric> {
        val otlpDimensions = metricDimensions.values.map { it.asOtlpKeyValue() }
        return sequence {
            for ((measurementName, value) in this@asGoofyOtlpMetricSequence.metricMeasurements) {
                yield(
                    metric {
                        // name: format!("{metric_name}_{measurement_name}", metric_name = datum.metric, measurement_name=name),
                        name = "${this@asGoofyOtlpMetricSequence.name}_$measurementName"
                        unit = "1"
                        gauge = gauge {
                            this.dataPoints.add(newNumberDataPoint(value, timestampNanos, (System.nanoTime() - startNanoTime).nanoseconds, otlpDimensions.asIterable()))
                        }
                    }
                )
            }
            for ((measurementName, value) in this@asGoofyOtlpMetricSequence.metricDistributions) {
                yield(
                    metric {
                        // name: format!("{metric_name}_{measurement_name}", metric_name = datum.metric, measurement_name=name),
                        name = "${this@asGoofyOtlpMetricSequence.name}_$measurementName"
                        unit = "1"
                        histogram = asOtlpHistogram(otlpDimensions, value)
                    }
                )
            }
        }
    }

    private fun newNumberDataPoint(value: Number, timestampNanos: Long, aggregationWidth: Duration, dimensions: Iterable<KeyValue>) = numberDataPoint {
        this.timeUnixNano = timestampNanos
        this.startTimeUnixNano = timestampNanos - aggregationWidth.inWholeNanoseconds
        attributes.addAll(dimensions)
        if (value is Long) {
            asInt = value
        } else {
            asDouble = value.toDouble()
        }
    }

    private fun Metrics.asOtlpHistogram(
        otlpDimensions: Iterable<KeyValue>,
        value: Long
    ) = histogram {
        // Because cumulative is bullshit for service metrics. Change my mind.
        aggregationTemporality = AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA
        dataPoints.add(
            histogramDataPoint {
                attributes.addAll(otlpDimensions)
                startTimeUnixNano = timestampNanos - (System.nanoTime() - startNanoTime) // approximate, whatever.
                timeUnixNano = timestampNanos
                count = 1
                explicitBounds.add(bucket(value).toDouble())
                bucketCounts.add(1)
                bucketCounts.add(0) // otlp go die in a fire
            }
        )
    }

    private fun Aggregation.Histogram.asOtlpHistogram(
        otlpDimensions: Iterable<KeyValue>,
        timestampNanos: Long,
        aggregationWidth: Duration,
    ) = histogram {
        // Because cumulative is bullshit for service metrics. Change my mind.
        aggregationTemporality = AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA
        dataPoints.add(
            histogramDataPoint {
                attributes.addAll(otlpDimensions)
                startTimeUnixNano = timestampNanos - aggregationWidth.inWholeNanoseconds
                timeUnixNano = timestampNanos
                val sorted = this@asOtlpHistogram.bucketCounts.toSortedMap()
                count = this@asOtlpHistogram.bucketCounts.values.sumOf { it.sum() }
                explicitBounds.addAll(sorted.keys.asSequence().map { it.toDouble() }.asIterable())
                bucketCounts.addAll(sorted.values.map { it.sum() })
                bucketCounts.add(0) // because OTLP is _stupid_ and defined histogram format to have an implicit infinity bucket.
            }
        )
    }

    private val library = instrumentationScope {
        name = "goodmetrics_kotlin"
        version = OpentelemetryClient::class.java.`package`.implementationVersion ?: "development"
    }

    private val prescientResource by lazy {
        when (prescientDimensions) {
            is PrescientDimensions.AsDimensions -> {
                null
            }
            is PrescientDimensions.AsResource -> {
                resource {
                    attributes.addAll(prescientDimensions.resourceDimensions.asOtlpDimensions().asIterable())
                }
            }
        }
    }

    private fun Map<String, Metrics.Dimension>.asOtlpDimensions(): Sequence<KeyValue> = sequence {
        for (dimension in this@asOtlpDimensions) {
            yield(dimension.value.asOtlpKeyValue())
        }
    }

    private fun Metrics.Dimension.asOtlpKeyValue(): KeyValue = keyValue {
        key = this@asOtlpKeyValue.name
        when (val v = this@asOtlpKeyValue) {
            is Metrics.Dimension.Boolean -> {
                value = anyValue { boolValue = v.value }
            }
            is Metrics.Dimension.Number -> {
                value = anyValue { intValue = v.value }
            }
            is Metrics.Dimension.String -> {
                value = anyValue { stringValue = v.value }
            }
        }
    }
}
