package goodmetrics

import goodmetrics.downstream.CompressionMode
import goodmetrics.downstream.GoodmetricsClient
import goodmetrics.downstream.GrpcTrailerLoggerInterceptor
import goodmetrics.downstream.OpentelemetryClient
import goodmetrics.downstream.PrescientDimensions
import goodmetrics.downstream.SecurityMode
import goodmetrics.io.opentelemetry.proto.metrics.v1.ResourceMetrics
import goodmetrics.pipeline.AggregatedBatch
import goodmetrics.pipeline.Aggregator
import goodmetrics.pipeline.BatchSender.Companion.launchSender
import goodmetrics.pipeline.Batcher
import goodmetrics.pipeline.MetricsSink
import goodmetrics.pipeline.SynchronizingBuffer
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.stub.MetadataUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class ConfiguredMetrics(
    val unaryMetricsFactory: MetricsFactory,
    val preaggregatedMetricsFactory: MetricsFactory,
)

class MetricsSetups private constructor() {
    companion object {
        fun CoroutineScope.goodMetrics(goodmetricsHost: String = "localhost", port: Int = 9573, aggregationWidth: Duration = 10.seconds): ConfiguredMetrics {
            val unaryIncomingBuffer = SynchronizingBuffer()
            val unaryFactory = MetricsFactory(unaryIncomingBuffer, timeSource = NanoTimeSource.preciseNanoTime, totaltimeType = MetricsFactory.TotaltimeType.DistributionMicroseconds)

            val unaryBatcher = Batcher(unaryIncomingBuffer)
            launchSender(unaryBatcher, GoodmetricsClient.connect(goodmetricsHost, port)) { batch ->
                sendMetricsBatch(batch)
            }

            val preaggregatedIncomingBuffer = Aggregator(aggregationWidth)
            val preaggregatedFactory = MetricsFactory(preaggregatedIncomingBuffer, timeSource = NanoTimeSource.fastNanoTime, totaltimeType = MetricsFactory.TotaltimeType.DistributionMicroseconds)

            val preaggregatedBatcher = Batcher(preaggregatedIncomingBuffer)
            launchSender(preaggregatedBatcher, GoodmetricsClient.connect(goodmetricsHost, port)) { batch ->
                sendPreaggregatedMetrics(batch)
            }

            return ConfiguredMetrics(
                unaryFactory,
                preaggregatedFactory,
            )
        }

        /**
         * preaggregated metrics in lightstep appear as Distributions for `Metrics::distribution`s
         * and as Delta temporality Sums for `Metrics::measure`ments.
         *
         * raw, unary metrics in lightstep also appear as Distribution for `Metrics::distribution`s
         * however `Metrics::measure`ments appear as Delta temporality Gauge so you can look at those
         * values with more flexibility in a way that makes more sense for raw emissions. It's a sharper
         * edged tool here, and there might be a better representation - to which goodmetrics will change
         * upon discovery.
         */
        fun CoroutineScope.lightstepNativeOtlp(
            lightstepAccessToken: String,
            prescientDimensions: PrescientDimensions,
            aggregationWidth: Duration,
            logError: (message: String, exception: Exception) -> Unit,
            onLightstepTrailers: (Status, Metadata) -> Unit = { status, trailers ->
                println("got trailers from lightstep. Status: $status, Trailers: $trailers")
            },
            lightstepUrl: String = "ingest.lightstep.com",
            lightstepPort: Int = 443,
            lightstepConnectionSecurityMode: SecurityMode = SecurityMode.Tls,
            timeout: Duration = 5.seconds,
            unaryBatchSizeMaxMetricsCount: Int = 1000,
            unaryBatchMaxAge: Duration = 10.seconds,
            preaggregatedBatchMaxMetricsCount: Int = 1000,
            preaggregatedBatchMaxAge: Duration = 10.seconds,
            onSendUnary: (List<Metrics>) -> Unit = {},
            onSendPreaggregated: (List<AggregatedBatch>) -> Unit = {},
            /**
             * This is verbose but can be helpful when debugging lightstep data format issues.
             * It shows you exactly what protocol buffers structure is sent.
             * Log with caution.
             */
            logRawPayload: (ResourceMetrics) -> Unit = {},
            /**
             * Lightstep claims:
             * > there is no compression on this traffic; I'm not sure what SDK you are using or if hand rolled but if you turn on compression this will go away.
             * So I'll default the special lightstep configuration to use gzip compression. You can disable this if you want.
             */
            compressionMode: CompressionMode = CompressionMode.Gzip,
        ): ConfiguredMetrics {
            val client = opentelemetryClient(
                lightstepAccessToken,
                lightstepUrl,
                lightstepPort,
                prescientDimensions,
                lightstepConnectionSecurityMode,
                onLightstepTrailers,
                timeout,
                logRawPayload,
                compressionMode,
            )

            val unarySink = configureBatchedUnaryLightstepSink(unaryBatchSizeMaxMetricsCount, unaryBatchMaxAge, client, logError, onSendUnary)
            val preaggregatedSink = configureBatchedPreaggregatedLightstepSink(aggregationWidth, preaggregatedBatchMaxMetricsCount, preaggregatedBatchMaxAge, client, logError, onSendPreaggregated)

            val unaryFactory = MetricsFactory(
                sink = unarySink,
                timeSource = NanoTimeSource.preciseNanoTime,
                totaltimeType = MetricsFactory.TotaltimeType.DistributionMicroseconds,
            )
            val preaggregatedFactory = MetricsFactory(
                sink = preaggregatedSink,
                timeSource = NanoTimeSource.fastNanoTime,
                totaltimeType = MetricsFactory.TotaltimeType.DistributionMicroseconds,
            )
            return ConfiguredMetrics(
                unaryMetricsFactory = unaryFactory,
                preaggregatedMetricsFactory = preaggregatedFactory,
            )
        }

        /**
         * The simplest configuration of metrics - it sends what you record, when you finish
         * recording it.
         *
         * Calling `metricsFactory.record { metrics -> [...] }` will see goodmetrics invoke
         * lightstep's ingest API _synchronously_ within the `}` scope end. If you are using
         * this in Lambda to record an execution, it will report before the execution completes.
         *
         * If you want preaggregated metrics in lambda or multiple recorded workflows per lambda
         * execution you might need to do some work - but probably you just wish you could emit
         * 1 row with a bunch of measurements per execution and this does that.
         */
        fun lightstepNativeOtlpButItSendsMetricsUponRecordingForLambda(
            lightstepAccessToken: String,
            prescientDimensions: PrescientDimensions,
            logError: (message: String, exception: Exception) -> Unit,
            onLightstepTrailers: (Status, Metadata) -> Unit = { status, trailers ->
                println("got trailers from lightstep. Status: $status, Trailers: $trailers")
            },
            lightstepUrl: String = "ingest.lightstep.com",
            lightstepPort: Int = 443,
            lightstepConnectionSecurityMode: SecurityMode = SecurityMode.Tls,
            timeout: Duration = 5.seconds,
            onSendUnary: (List<Metrics>) -> Unit = {},
            compressionMode: CompressionMode = CompressionMode.None,
        ): MetricsFactory {
            val client = opentelemetryClient(
                lightstepAccessToken,
                lightstepUrl,
                lightstepPort,
                prescientDimensions,
                lightstepConnectionSecurityMode,
                onLightstepTrailers,
                timeout,
                compressionMode = compressionMode,
            )

            val unarySink = object : MetricsSink {
                override fun emit(metrics: Metrics) {
                    runBlocking {
                        onSendUnary(listOf(metrics))
                        try {
                            client.sendMetricsBatch(listOf(metrics))
                        } catch (e: Exception) {
                            logError("error while sending blocking metrics", e)
                        }
                    }
                }

                override fun close() {
                    client.close()
                }
            }

            return MetricsFactory(
                sink = unarySink,
                timeSource = NanoTimeSource.preciseNanoTime,
                totaltimeType = MetricsFactory.TotaltimeType.DistributionMicroseconds,
            )
        }

        private fun CoroutineScope.configureBatchedUnaryLightstepSink(
            batchSize: Int,
            batchMaxAge: Duration,
            client: OpentelemetryClient,
            logError: (message: String, exception: Exception) -> Unit,
            onSendUnary: (List<Metrics>) -> Unit,
        ): SynchronizingBuffer {
            val unarySink = SynchronizingBuffer()
            val unaryBatcher = Batcher(
                unarySink,
                batchSize = batchSize,
                batchAge = batchMaxAge,
            )

            launch {
                // Launch the sender on a background coroutine.
                unaryBatcher.consume()
                    .collect { metrics ->
                        onSendUnary(metrics)
                        try {
                            client.sendMetricsBatch(metrics)
                        } catch (e: Exception) {
                            logError("error sending unary batch", e)
                        }
                    }
            }
            return unarySink
        }

        private fun CoroutineScope.configureBatchedPreaggregatedLightstepSink(
            aggregationWidth: Duration,
            batchSize: Int,
            batchMaxAge: Duration,
            client: OpentelemetryClient,
            logError: (message: String, exception: Exception) -> Unit,
            onSendPreaggregated: (List<AggregatedBatch>) -> Unit,
        ): Aggregator {
            val sink = Aggregator(aggregationWidth = aggregationWidth)
            val batcher = Batcher(
                sink,
                batchSize = batchSize,
                batchAge = batchMaxAge,
            )

            launch {
                // Launch the sender on a background coroutine.
                batcher.consume()
                    .collect { metrics ->
                        onSendPreaggregated(metrics)
                        try {
                            client.sendPreaggregatedBatch(metrics)
                        } catch (e: Exception) {
                            logError("error sending preaggregated batch", e)
                        }
                    }
            }
            return sink
        }

        private fun opentelemetryClient(
            lightstepAccessToken: String,
            lightstepUrl: String,
            lightstepPort: Int,
            prescientDimensions: PrescientDimensions,
            lightstepConnectionSecurityMode: SecurityMode,
            onLightstepTrailers: (Status, Metadata) -> Unit,
            timeout: Duration,
            logRawPayload: (ResourceMetrics) -> Unit = { },
            compressionMode: CompressionMode,
        ): OpentelemetryClient {
            val authHeader = Metadata()
            authHeader.put(
                Metadata.Key.of("lightstep-access-token", Metadata.ASCII_STRING_MARSHALLER),
                lightstepAccessToken
            )

            return OpentelemetryClient.connect(
                sillyOtlpHostname = lightstepUrl,
                port = lightstepPort,
                prescientDimensions = prescientDimensions,
                securityMode = lightstepConnectionSecurityMode,
                interceptors = listOf(
                    MetadataUtils.newAttachHeadersInterceptor(authHeader),
                    GrpcTrailerLoggerInterceptor(onLightstepTrailers),
                ),
                timeout = timeout,
                logRawPayload = logRawPayload,
                compressionMode = compressionMode,
            )
        }
    }
}
