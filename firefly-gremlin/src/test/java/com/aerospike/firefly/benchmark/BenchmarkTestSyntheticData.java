package com.aerospike.firefly.benchmark;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
// Takes about 30 minutes to run in GitHub actions.
@Measurement(iterations = 1, time = 45, timeUnit = TimeUnit.SECONDS)
public class BenchmarkTestSyntheticData {
    // Sample usage: mvn test -Dfirefly.host=172.17.0.3 -Ddocker.benchmark=1 -Dtest=BenchmarkTestSyntheticData -DfailIfNoTests=false --no-transfer-progress
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTestSyntheticData.class);
    private static final String HOST = BenchmarkTestUtils.getHost();
    private static final int PORT = 8182;
    private static final Integer THREADS = BenchmarkTestUtils.getThreads();
    private Cluster cluster = null;
    private GraphTraversalSource g = null;
    private List<Object> deviceIds = null;
    private List<Object> householdIds = null;
    private Random random = new Random();
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false).maxConnectionPoolSize(THREADS).minConnectionPoolSize(THREADS);

    // Setup for the benchmark. Note, BeforeClass won't work to set static variables
    // because jmh launches a separate JVM for the benchmark and the @BeforeClass
    // annotation is ignored.
    @Setup
    public void setup(final BenchmarkParams benchmarkParams) {
        LOG.info("Creating the Cluster (setup).");
        cluster = BUILDER.create();

        LOG.info("Creating the GraphTraversalSource (setup).");
        g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        // Try to get devices and households from the graph, hardcode in case we fail to get them.
        deviceIds = g.V().hasLabel("Device").limit(100).id().toList();
        householdIds = g.V().hasLabel("Hosehold").limit(100).id().toList();
    }

    // Teardown for benchmark.
    @TearDown
    public void tearDown() {
        if (g != null) {
            try {
                LOG.info("Closing the GraphTraversalSource.");
                g.close();
            } catch (Exception e) {
                LOG.info("Failed to close the GraphTraversalSource.", e);
            }
        }
        if (cluster != null) {
            try {
                LOG.info("Closing the Cluster.");
                cluster.close();
            } catch (Exception e) {
                LOG.info("Failed to close the Cluster.", e);
            }
        }
    }

    // Use junit test to hook into maven nicely. This test will
    // use the benchmark framework which runs the benchmarks in a
    // separate JVM.
    @Test
    public void fireflyBenchmark() throws RunnerException {
        final ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .include(BenchmarkTestSyntheticData.class.getSimpleName())
                .detectJvmArgs()
                .forks(4)
                .threads(THREADS)
                .timeout(TimeValue.minutes(2)); // Timeout
        BenchmarkTestUtils.appendJmhOptionsBuilder(optBuilder);
        Options opt = optBuilder.build();
        Collection<RunResult> runResult = new Runner(opt).run();

        LOG.info("Results:");
        LOG.info(runResult.toString());

    }

    @Benchmark
    public void benchmark_g_V_inOwns_inPartOf_outPartOf_outOwns(final Blackhole blackhole) {
        // Start with device, go 'in' to owner, go 'in' to household, go 'out' to all owners, go 'out' to all devices.
        Object id = deviceIds.get(random.nextInt(deviceIds.size()));
        final List<Vertex> vertices = g.V(id).
                in("owns").
                out("partOf").
                in("partOf").
                out("owns").toList();
        if (vertices.size() > 0) {
            blackhole.consume(vertices);
        } else {
            throw new RuntimeException("No vertices found.");
        }
    }

    @Benchmark
    public void benchmark_g_V_inOwns_outOwns(final Blackhole blackhole) {
        // Start with device, go 'in' to owner, go 'out' to all devices.
        Object id = deviceIds.get(random.nextInt(deviceIds.size()));
        final List<Vertex> vertices = g.V(id).
                in("owns").
                out("owns").toList();
        if (vertices.size() > 0) {
            blackhole.consume(vertices);
        } else {
            throw new RuntimeException("No vertices found.");
        }
    }

    @Benchmark
    public void benchmark_g_V_outPartOf_outOwns(final Blackhole blackhole) {
        // Start with household, go 'out' to all people, go 'out' to all devices.
        Object id = householdIds.get(random.nextInt(householdIds.size()));
        final List<Vertex> vertices = g.V(id).
                in("partOf").
                out("owns").toList();
        if (vertices.size() > 0) {
            blackhole.consume(vertices);
        } else {
            throw new RuntimeException("No vertices found.");
        }
    }
}
