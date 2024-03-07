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
import goodmetrics.pipeline.bucketBelow
import io.grpc.CallOptions
import io.grpc.ClientInterceptor
import io.grpc.ManagedChannel
import io.grpc.netty.GrpcSslContexts
import io.grpc.netty.NettyChannelBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder
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

sealed interface CompressionMode {
    object None : CompressionMode
    object Gzip : CompressionMode
    data class IKnowWhatIWant(val explicitMode: String) : CompressionMode
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
    private val logRawPayload: (ResourceMetrics) -> Unit = { },
    private val compressionMode: CompressionMode,
) : AutoCloseable {
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
            logRawPayload: (ResourceMetrics) -> Unit = { },
            compressionMode: CompressionMode = CompressionMode.None,
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
            return OpentelemetryClient(channelBuilder.build(), prescientDimensions, timeout, logRawPayload, compressionMode)
        }
    }
    private fun stub(): MetricsServiceGrpcKt.MetricsServiceCoroutineStub {
        val defaultCallOptions = CallOptions.DEFAULT
            .withDeadlineAfter(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        val callOptions = when (compressionMode) {
            CompressionMode.None -> defaultCallOptions
            CompressionMode.Gzip -> defaultCallOptions.withCompression("gzip")
            is CompressionMode.IKnowWhatIWant -> defaultCallOptions.withCompression(compressionMode.explicitMode)
        }

        return MetricsServiceGrpcKt.MetricsServiceCoroutineStub(channel, callOptions)
    }

    suspend fun sendMetricsBatch(batch: List<Metrics>) {
        val resourceMetricsBatch = asResourceMetrics(batch)
        logRawPayload(resourceMetricsBatch)
        stub().export(
            exportMetricsServiceRequest {
                resourceMetrics.add(resourceMetricsBatch)
            }
        )
    }

    suspend fun sendPreaggregatedBatch(batch: List<AggregatedBatch>) {
        val resourceMetricsBatch = asResourceMetricsFromBatch(batch)
        logRawPayload(resourceMetricsBatch)
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
        this.scopeMetrics.add(asScopeMetrics(batch))
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
        yield(statisticSetDataPointGauge(metric, measurementName, "min", min, timestampNanos, aggregationWidth, dimensions))
        yield(statisticSetDataPointGauge(metric, measurementName, "max", max, timestampNanos, aggregationWidth, dimensions))
        yield(statisticSetDataPointCounter(metric, measurementName, "sum", sum, timestampNanos, aggregationWidth, dimensions))
        yield(statisticSetDataPointCounter(metric, measurementName, "count", count, timestampNanos, aggregationWidth, dimensions))
    }

    private fun statisticSetDataPointCounter(
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
            isMonotonic = true
            // because cumulative is bullshit
            aggregationTemporality = AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA
            dataPoints.add(newNumberDataPoint(value, timestampNanos, aggregationWidth, dimensions))
        }
    }

    private fun statisticSetDataPointGauge(
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
        gauge = gauge {
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
        if (value is Long || value is LongAdder) {
            asInt = value.toLong()
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

                val bucketValue = bucket(value)
                if (0 < bucketValue) {
                    // This little humdinger is here so Lightstep can interpret the boundary for the _real_ measurement
                    // below. It's similar to the 0 that opentelemetry demands, but different in that it is actually a
                    // reasonable ask.
                    // Lightstep has an internal representation of histograms & while I don't pretend  to understand
                    // how they've implemented them, they told me that they interpret the absence of a lower bounding
                    // bucket as an infinite lower bound. That's not consistent with my read of otlp BUT it makes
                    // infinitely more sense than imposing an upper infinity bucket upon your protocol.
                    // Prometheus is a cataclysm from which there is no redemption: It ruins developers' minds with
                    // its broken and much lauded blunders; it shames my profession by its protocol as well as those
                    // spawned through its vile influence and disappoints the thoughtful by its existence.
                    // But, you know, this particular thing for Lightstep seems fine because there's technical merit.
                    explicitBounds.add(bucketBelow(value).toDouble())
                    bucketCounts.add(0)
                }

                explicitBounds.add(bucketValue.toDouble())
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
                for ((bucket, count) in sorted) {
                    val below = bucketBelow(bucket)
                    if (0 < below && !this@asOtlpHistogram.bucketCounts.containsKey(below)) {
                        // And THIS little humdinger is here so Lightstep can interpret the boundary for all non-zero
                        // buckets. Lightstep histogram implementation wants non-zero-count ranges to have lower bounds.
                        // Not how I've done histograms in the past but :shrug: whatever, looks like the opentelemetry
                        // metrics spec is at fault for this one; they refused to improve the specification from
                        // openmetrics, which was bastardized in turn by that root of all monitoring evil: Prometheus.
                        // Lightstep is a business which must adhere to de-facto standards, so I don't fault them for
                        // this; though I would love it if they were to also adopt a good protocol.
                        explicitBounds.add(below.toDouble())
                        bucketCounts.add(0L)
                    }

                    explicitBounds.add(bucket.toDouble())
                    bucketCounts.add(count.sum())
                }

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

    override fun close() {
        channel.shutdown()
    }
}
