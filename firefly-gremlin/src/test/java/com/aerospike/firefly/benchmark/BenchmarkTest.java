package com.aerospike.firefly.benchmark;


import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
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
                .timeout(TimeValue.minutes(10)) // Timeout
                .build();
        new Runner(opt).run();
    }

    // TODO: Benchmark should ONLY contain the code that is being benchmarked. I.E g.V() code.
    @Benchmark
    public void testBenchmark1(final Blackhole blackhole) throws Exception {
        System.out.println("Benchmark 1");
        final Graph graph = GraphTraversalSourceFactory.createGraphTraversalSource(GRAPH_TYPE);
        GraphTraversalSource g = graph.traversal();
        List<Vertex> vertices = g.V().has("code", "AUS").out().out().has("code", "SEA").toList();
        blackhole.consume(vertices);
        graph.close();
    }

    // TODO: Benchmark should ONLY contain the code that is being benchmarked. I.E g.V() code.
    @Benchmark
    public void testBenchmark2(final Blackhole blackhole) throws Exception {
        System.out.println("Benchmark 2");
        final Graph graph = GraphTraversalSourceFactory.createGraphTraversalSource(GRAPH_TYPE);
        GraphTraversalSource g = graph.traversal();
        List<Vertex> vertices = g.V().has("code", "AUS").out().has("code", "SEA").toList();
        blackhole.consume(vertices);
        graph.close();
    }
}
