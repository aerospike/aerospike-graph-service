package com.aerospike.firefly.benchmark;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MINUTES)
public class BenchmarkTest {
    private static final GraphTraversalSourceFactory.GRAPH GRAPH_TYPE = GraphTraversalSourceFactory.GRAPH.FIREFLY;
    private static final GraphTraversalSourceFactory.DATASET DATASET_TYPE = GraphTraversalSourceFactory.DATASET.FLIGHTS;

    @BeforeClass
    public static void setup() {
        GraphTraversalSourceFactory.loadGraph(GRAPH_TYPE, DATASET_TYPE);
    }

    @Test
    public void fireflyBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkTest.class.getSimpleName())
                .forks(1)
                .timeout(TimeValue.seconds(20))
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void testBenchmark1(final Blackhole blackhole) {
        final GraphTraversalSource g = GraphTraversalSourceFactory.createGraphTraversalSource(GRAPH_TYPE);
        List<Vertex> vertices = g.V().has("code", "AUS").out().out().has("code", "SEA").toList();
        blackhole.consume(vertices);
    }

    @Benchmark
    public void testBenchmark2(final Blackhole blackhole) {
        final GraphTraversalSource g = GraphTraversalSourceFactory.createGraphTraversalSource(GRAPH_TYPE);
        List<Vertex> vertices = g.V().has("code", "AUS").out().has("code", "SEA").toList();
        blackhole.consume(vertices);
    }
}
