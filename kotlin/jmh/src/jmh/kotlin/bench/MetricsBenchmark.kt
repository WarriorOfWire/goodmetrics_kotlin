package bench

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit

import goodmetrics.Metrics
import goodmetrics.MetricsFactory
import goodmetrics.NanoTimeSource
import goodmetrics.TimestampAt
import goodmetrics.pipeline.MetricsSink
import org.openjdk.jmh.annotations.OperationsPerInvocation
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State

object Sink: MetricsSink {
    override fun emit(metrics: Metrics) {
    }
    override fun close() {
    }
}

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(1)
@Fork(value = 1, jvmArgs = ["-server"])
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
open class MetricsBenchmark {
    val metricsFactory = MetricsFactory(
        Sink,
        NanoTimeSource.fastNanoTime,
        MetricsFactory.TotaltimeType.None,
    )

    val m = metricsFactory.internals.getMetrics("benchmark", TimestampAt.Start)

    @Benchmark
    @OperationsPerInvocation(50)
    fun recordInt() {
        m.measure("lel", 42)
        m.measure("lel2", 42)
        m.measure("lel23", 42)
        m.measure("lel234", 42)
        m.measure("lel2345", 42)
        m.measure("lel23456", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
        m.measure("lel234567", 42)
    }
}
