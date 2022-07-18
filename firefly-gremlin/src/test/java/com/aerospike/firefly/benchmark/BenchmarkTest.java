package com.aerospike.firefly.benchmark;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.List;
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
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8182;
    private static final GraphLoader.DATASET DATASET_TYPE = GraphLoader.DATASET.FLIGHTS;
    private Cluster cluster = null;
    private GraphTraversalSource g = null;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);

    // Run before the class, this will run before all the benchmarks
    // and load the graph.
    @BeforeClass
    public static void load() {
        System.out.println("Creating the Cluster.");
        final Cluster cluster = BUILDER.create();

        System.out.println("Creating the GraphTraversalSource.");
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        System.out.println("Clearing the graph.");
        g.V().drop().iterate();

        System.out.println("Loading the graph.");
        GraphLoader.loadGraph(g, DATASET_TYPE);

        try {
            g.close();
        } catch (Exception e) {
            System.out.println("Failed to close the GraphTraversalSource.");
        }
        try {
            cluster.close();
        } catch (Exception e) {
            System.out.println("Failed to close the Cluster.");
        }
    }

    // Setup for the benchmark. Note, BeforeClass won't work to set static variables
    // because jmh launches a separate JVM for the benchmark and the @BeforeClass
    // annotation is ignored.
    @Setup
    public void setup(BenchmarkParams benchmarkParams) {
        System.out.println("Creating the Cluster (setup).");
        cluster = BUILDER.create();

        System.out.println("Creating the GraphTraversalSource (setup).");
        g = traversal().withRemote(DriverRemoteConnection.using(cluster));
    }

    // Teardown for benchmark.
    @TearDown
    public void tearDown() {
        if (g != null) {
            try {
                System.out.println("Closing the GraphTraversalSource.");
                g.close();
            } catch (Exception e) {
                System.out.println("Failed to close the GraphTraversalSource.");
            }
        }
        if (cluster != null) {
            try {
                System.out.println("Closing the Cluster.");
                cluster.close();
            } catch (Exception e) {
                System.out.println("Failed to close the Cluster.");
            }
        }
    }

    // Use junit test to hook into maven nicely. This test will
    // use the benchmark framework which runs the benchmarks in a
    // separate JVM.
    @Test
    public void fireflyBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkTest.class.getSimpleName())
                .forks(1)
                .timeout(TimeValue.minutes(10)) // Timeout
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void testBenchmark1(final Blackhole blackhole) {
        List<Vertex> vertices = g.V().has("code", "AUS").out().out().out().has("code", "SEA").toList();
        blackhole.consume(vertices);
    }
}
