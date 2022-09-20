package com.aerospike.firefly.benchmark;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

// TODO: We should tune these parameters to provide the benchmark
//       with results we like - likely add warmup and adjust
//       iterations.
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MINUTES)
public class BenchmarkTest {
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTest.class);
    private static final String HOST = BenchmarkTestUtils.getHost();
    private static final int PORT = 8182;
    private static final BenchmarkTestUtils.DATASET DATASET_TYPE = BenchmarkTestUtils.DATASET.FLIGHTS;
    private Cluster cluster = null;
    private GraphTraversalSource g = null;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
    public String host;

    // Run before the class, this will run before all the benchmarks
    // and load the graph.
    @BeforeClass
    public static void load() {
        System.out.println("Host: " + HOST);

        LOG.info("Creating the Cluster.");
        final Cluster cluster = BUILDER.create();

        LOG.info("Creating the GraphTraversalSource.");
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        BenchmarkTestUtils.loadGraph(g, DATASET_TYPE);

        try {
            g.close();
        } catch (Exception e) {
            LOG.info("Failed to close the GraphTraversalSource.");
        }
        try {
            cluster.close();
        } catch (Exception e) {
            LOG.info("Failed to close the Cluster.");
        }
    }

    // Setup for the benchmark. Note, BeforeClass won't work to set static variables
    // because jmh launches a separate JVM for the benchmark and the @BeforeClass
    // annotation is ignored.
    @Setup
    public void setup(final BenchmarkParams benchmarkParams) {
        LOG.info("Creating the Cluster (setup).");
        cluster = BUILDER.create();

        LOG.info("Creating the GraphTraversalSource (setup).");
        g = traversal().withRemote(DriverRemoteConnection.using(cluster));
    }

    // Teardown for benchmark.
    @TearDown
    public void tearDown() {
        if (g != null) {
            try {
                LOG.info("Closing the GraphTraversalSource.");
                g.close();
            } catch (Exception e) {
                LOG.info("Failed to close the GraphTraversalSource.");
            }
        }
        if (cluster != null) {
            try {
                LOG.info("Closing the Cluster.");
                cluster.close();
            } catch (Exception e) {
                LOG.info("Failed to close the Cluster.");
            }
        }
    }

    // Use junit test to hook into maven nicely. This test will
    // use the benchmark framework which runs the benchmarks in a
    // separate JVM.
    @Test
    public void fireflyBenchmark() throws RunnerException {
        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .include(BenchmarkTest.class.getSimpleName())
                .detectJvmArgs()
                .forks(4)
                .timeout(TimeValue.minutes(2)); // Timeout
        BenchmarkTestUtils.appendJmhOptionsBuilder(optBuilder);
        Options opt = optBuilder.build();
        new Runner(opt).run();
    }

    @Benchmark
    public void benchmark_g_V_hasxcode_DFWx(final Blackhole blackhole) {
        final List<Vertex> vertices = g.V().has("code", "DFW").toList();
        blackhole.consume(vertices);
    }

    @Benchmark
    public void benchmark_g_V_hasxairport_code_DFWx(final Blackhole blackhole) {
        final List<Vertex> vertices = g.V().has("airport", "code", "DFW").toList();
        blackhole.consume(vertices);
    }

    @Benchmark
    public void benchmark_g_V_hasxcode_DFWx_outE_count(final Blackhole blackhole) {
        final long outECount = g.V().has("code", "DFW").outE().count().next();
        blackhole.consume(outECount);
    }

    @Benchmark
    public void benchmark_g_V_hasxcode_SFOx_out_out_out_hasxcode_YVRx(final Blackhole blackhole) {
        final List<Vertex> vertices = g.V().has("code", "SFO").out().out().out().has("code", "YVR").toList();
        blackhole.consume(vertices);
    }

    @Benchmark
    public void benchmark_g_V_hasxcode_SFOx_out_out_project_byxunfold_countx_byxunfold_hasxcountry_USx_count(final Blackhole blackhole) {
        final Map<String, Object> projectionMap = g.V().
                has("code", "SFO").
                out().out().
                dedup().fold().
                project("totalAirportCountFromSFO", "USAirportCountFromSFO").
                by(__.unfold().count()).
                by(__.unfold().has("country", "US").count()).next();
        blackhole.consume(projectionMap);
    }

    @Benchmark
    public void benchmark_g_e_hasxdist_gtx4000x_inV_values_dedup(final Blackhole blackhole) {
        final List<Object> cities = g.E().has("dist", P.gt(4000L)).inV().values("city").dedup().toList();
        blackhole.consume(cities);
    }

    @Benchmark
    public void benchmark_g_V_hasxcode_LHRx_outxroutex_hasxcountry_USx_valuesxcodex(final Blackhole blackhole) {
        final List<Object> codes = g.V().has("code", "LHR").out("route").has("country", "US").values("code").toList();
        blackhole.consume(codes);
    }

    @Benchmark
    public void benchmark_g_V_hasLabelxairportx_count(final Blackhole blackhole) {
        final long airportCount = g.V().hasLabel("airport").count().next();
        blackhole.consume(airportCount);
    }
}
