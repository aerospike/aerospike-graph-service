package com.aerospike.firefly.benchmark;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
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

import java.io.FileWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
public class BenchmarkTestIdentifiesTraversal {
    // Sample usage: mvn test -Dfirefly.host=172.17.0.3 -Ddocker.benchmark=1 -Dtest=BenchmarkTestIdentifiesTraversal -DfailIfNoTests=false --no-transfer-progress
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTestIdentifiesTraversal.class);
    private static final String HOST = BenchmarkTestUtils.getHost();
    private static final int PORT = 8182;
    private static final Mode MODE = BenchmarkTestUtils.getMode(LOG);
    private DriverRemoteConnection drc = null;
    private Client.ClusteredClient client = null;
    private Cluster cluster = null;
    private GraphTraversalSource g = null;
    private static final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
    private static final Map<String, String> testToTraversal = Map.ofEntries(
            Map.entry("benchmark_g_V_traversal_extra_optimized_0", "Identifies traversal (1) extra optimized"),
            Map.entry("benchmark_g_V_traversal_optimized_0", "Identifies traversal (1) optimized"),
            Map.entry("benchmark_g_V_traversal_unoptimized_0", "Identifies traversal (1) unoptimized"),
            Map.entry("benchmark_g_V_traversal_optimized_0_script", "Identifies traversal (1) optimized script"),
            Map.entry("benchmark_g_V_traversal_optimized_1", "Identifies traversal (2) optimized"),
            Map.entry("benchmark_g_V_traversal_unoptimized_1", "Identifies traversal (2) unoptimized"),
            Map.entry("benchmark_g_V_traversal_unoptimized_2", "Identifies traversal (3) unoptimized"),
            Map.entry("benchmark_g_V_traversal_unoptimized_2_profile", "Identifies traversal (3) unoptimized profile")
    );

    @Setup(Level.Invocation)

    public void loadData() {
        g.V().drop().iterate();
    }

    // Setup for the benchmark. Note, BeforeClass won't work to set static variables
    // because jmh launches a separate JVM for the benchmark and the @BeforeClass
    // annotation is ignored.
    @Setup
    public void setup(final BenchmarkParams benchmarkParams) {
        LOG.info("Creating the Cluster (setup).");
        cluster = BUILDER.create();

        LOG.info("Creating the GraphTraversalSource (setup).");
        drc = DriverRemoteConnection.using(cluster);
        client = cluster.connect();
        g = traversal().withRemote(drc);
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
        final ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .include(BenchmarkTestIdentifiesTraversal.class.getSimpleName())
                .detectJvmArgs()
                .forks(1)
                .threads(1)
                .mode(MODE)
                .measurementIterations(10)
                .measurementTime(TimeValue.seconds(30))
                .timeout(TimeValue.minutes(1)); // Timeout
        BenchmarkTestUtils.appendJmhOptionsBuilder(optBuilder);
        Options opt = optBuilder.build();
        Collection<RunResult> runResult = new Runner(opt).run();

        // Format:
        //[
        //  {
        //          "name": "Chart Title",
        //          "unit": "Chart Unit",
        //          "value": 100,
        //          "range": "3",
        //          "extra": "Value for Tooltip: 25\nOptional Num #2: 100\nAnything Else!"
        //  },
        //  ...
        //]

        final JSONArray root = new JSONArray();
        runResult.forEach(result -> {
            JSONObject obj = new JSONObject();
            obj.put("name", testToTraversal.get(result.getPrimaryResult().getLabel()));
            obj.put("unit", result.getPrimaryResult().getScoreUnit());
            obj.put("value", result.getPrimaryResult().getScore());
            if (Double.isFinite(result.getPrimaryResult().getScoreError())) {
                obj.put("range", result.getPrimaryResult().getScoreError());
            } else {
                obj.put("range", "0");
            }
            obj.put("extra", result.getPrimaryResult().getStatistics());
            root.put(obj);
        });
        try (final FileWriter file = new FileWriter("target/jmh-result.json")) {
            file.write(root.toString());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    final String id1 = "< ID_1 >";
    final String id2 = "< ID_2 >";
    final String id3 = "< ID_3 >";
    final String id4 = "< ID_4 >";
    final String id5 = "< ID_5 >";
    final String id6 = "< ID_6 >";
    final String id7 = "< ID_7 >";
    final String id8 = "< ID_8 >";
    final String id9 = "< ID_9 >";
    final String entity1 = "< ENTITY_1 >";
    final String entity2 = "< ENTITY_2 >";
    final String entity3 = "< ENTITY_3 >";
    final String entity4 = "< ENTITY_4 >";

    @Benchmark
    public void benchmark_g_V_traversal_unoptimized_0(final Blackhole bh) {
        final List<Vertex> result =
                g.V(id1).
                        fold().coalesce(
                                __.unfold(),
                                __.addV("entity_link").
                                        property(T.id, "< ID_1 >").
                                        property("type", "id").
                                        property("opt_ind", "0").
                                        property("first_seen", 0L).
                                        property("last_seen", 0L))
                        .sideEffect(__.V(id1).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                        .sideEffect(__.V(entity2).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("address").
                                                property(T.id, entity2).
                                                property("type", "entity").
                                                property("first_seen", 0L)))
                        .sideEffect(__.V(entity2).
                                coalesce(__.properties("internal dataset").drop()))
                        .sideEffect(
                                __.V(entity2).
                                        inE("identifies").outV().hasId(id1).
                                        fold().coalesce(
                                                __.unfold(),
                                                __.addE("identifies").
                                                        property("first_seen", 0L).
                                                        from(__.V(id1)).
                                                        to(__.V(entity2))))
                        .sideEffect(
                                __.V(entity2).
                                        inE("identifies").
                                        as("edge").
                                        outV().
                                        hasId(id1).
                                        select("edge").
                                        properties("internal dataset").drop()).toList();
        bh.consume(result);
    }

    @Benchmark
    public void benchmark_g_V_traversal_optimized_0(final Blackhole blackhole) {
        List<Vertex> vertexList =
                g.V(id1).
                        fold().coalesce(
                                __.unfold(),
                                __.addV("entity_link").
                                        property(T.id, id1).
                                        property("type", "id").
                                        property("opt_ind", "0").
                                        property("first_seen", 0L).
                                        property("last_seen", 0L)).
                        sideEffect(
                                __.V(id1).coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L))).
                        sideEffect(
                                __.V(entity2).fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addV("address").property(T.id, entity2).
                                                        property("type", "entity").
                                                        property("first_seen", 0L)).
                                        sideEffect(__.properties("internal dataset").drop()).
                                        sideEffect(
                                                __.in("identifies").hasId(id1).
                                                        fold().coalesce(
                                                                __.unfold(),
                                                                __.addE("identifies").property("first_seen", 0L).
                                                                        from(__.V(id1)).
                                                                        to(__.V(entity2)))).
                                        sideEffect(
                                                __.inE("identifies").
                                                        where(__.outV().hasId(id1)).
                                                        properties("internal dataset").
                                                        drop())).toList();
        blackhole.consume(vertexList);
    }

    final String script = "g.V('" + id1 + "').\n" +
            "                        fold().coalesce(\n" +
            "                                __.unfold(),\n" +
            "                                __.addV(\"entity_link\").\n" +
            "                                        property(T.id, '" + id1 + "').\n" +
            "                                        property(\"type\", \"id\").\n" +
            "                                        property(\"opt_ind\", \"0\").\n" +
            "                                        property(\"first_seen\", 0L).\n" +
            "                                        property(\"last_seen\", 0L)).\n" +
            "                        sideEffect(\n" +
            "                                __.V('" + id1 + "').coalesce(\n" +
            "                                        __.properties(\"internal dataset\").drop(),\n" +
            "                                        __.property(\"last_seen\", 0L))).\n" +
            "                        sideEffect(\n" +
            "                                __.V('" + entity2 + "').fold().\n" +
            "                                        coalesce(\n" +
            "                                                __.unfold(),\n" +
            "                                                __.addV(\"address\").property(T.id, '" + entity2 + "').\n" +
            "                                                        property(\"type\", \"entity\").\n" +
            "                                                        property(\"first_seen\", 0L)).\n" +
            "                                        sideEffect(__.properties(\"internal dataset\").drop()).\n" +
            "                                        sideEffect(\n" +
            "                                                __.in(\"identifies\").hasId('" + id1 + "').\n" +
            "                                                        fold().coalesce(\n" +
            "                                                                __.unfold(),\n" +
            "                                                                __.addE(\"identifies\").property(\"first_seen\", 0L).\n" +
            "                                                                        from(__.V('" + id1 + "')).\n" +
            "                                                                        to(__.V('" + entity2 + "')))).\n" +
            "                                        sideEffect(\n" +
            "                                                __.inE(\"identifies\").\n" +
            "                                                        where(__.outV().hasId('" + id1 + "')).\n" +
            "                                                        properties(\"internal dataset\").\n" +
            "                                                        drop()))";

    @Benchmark
    public void benchmark_g_V_traversal_optimized_0_script(final Blackhole blackhole) {
        blackhole.consume(client.submit(script).all().join());
    }

    @Benchmark
    public void benchmark_g_V_traversal_unoptimized_1(final Blackhole bh) {
        final List<Vertex> result =
                g.V(id1).fold().
                        coalesce(
                                __.unfold(),
                                __.addV("entity_link").
                                        property(T.id, id1).
                                        property("type", "id").
                                        property("opt_ind", "0").
                                        property("first_seen", 0L).
                                        property("last_seen", 0L))
                        .sideEffect(
                                __.V(id1).
                                        coalesce(
                                                __.properties("internal dataset").drop(),
                                                __.property("last_seen", 0L)))
                        .sideEffect(
                                __.V(id4).fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addV("ccsid_e").
                                                        property(T.id, id4).
                                                        property("type", "id").
                                                        property("opt_ind", "0").
                                                        property("first_seen", 0L).
                                                        property("last_seen", 0L)))
                        .sideEffect(
                                __.V(id4).
                                        coalesce(
                                                __.properties("internal dataset").drop(),
                                                __.property("last_seen", 0L)))
                        .sideEffect(
                                __.V(id6).fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addV("dl_tid_e").
                                                        property(T.id, id6).
                                                        property("type", "id").
                                                        property("opt_ind", "0").
                                                        property("first_seen", 0L).
                                                        property("last_seen", 0L)))
                        .sideEffect(
                                __.V(id6).
                                        coalesce(
                                                __.properties("internal dataset").drop(),
                                                __.property("last_seen", 0L)))
                        .sideEffect(
                                __.V(entity2).fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addV("address").
                                                        property(T.id, entity2).
                                                        property("type", "entity").
                                                        property("first_seen", 0L)))
                        .sideEffect(
                                __.V(entity2).
                                        coalesce(
                                                __.properties("internal dataset").drop()))
                        .sideEffect(
                                __.V(entity2).
                                        inE("identifies").
                                        outV().hasId(id1).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("identifies").
                                                        property("first_seen", 0L).
                                                        from(
                                                                __.V(id1)).
                                                        to(
                                                                __.V(entity2))))
                        .sideEffect(
                                __.V(entity2).
                                        inE("identifies").
                                        as("edge").
                                        outV().hasId(id1).
                                        select("edge").
                                        properties("internal dataset").drop())
                        .sideEffect(
                                __.V(entity4).fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addV("sub_account").
                                                        property(T.id, entity4).
                                                        property("type", "entity").
                                                        property("first_seen", 0L)))
                        .sideEffect(
                                __.V(entity4).
                                        coalesce(
                                                __.properties("internal dataset").drop()))
                        .sideEffect(
                                __.V(entity4).
                                        inE("identifies").
                                        outV().hasId(id4).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("identifies").
                                                        property("first_seen", 0L).
                                                        from(
                                                                __.V(id4)).
                                                        to(
                                                                __.V(entity4))))
                        .sideEffect(
                                __.V(entity4).
                                        inE("identifies").
                                        as("edge").
                                        outV().hasId(id4).
                                        select("edge").
                                        properties("internal dataset").drop())
                        .sideEffect(
                                __.V(entity4).
                                        inE("identifies").
                                        outV().hasId(id6).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("identifies").
                                                        property("first_seen", 0L).
                                                        from(
                                                                __.V(id6)).
                                                        to(
                                                                __.V(entity4))))
                        .sideEffect(
                                __.V(entity4).
                                        inE("identifies").as("edge").
                                        outV().hasId(id6).
                                        select("edge").
                                        properties("internal dataset").drop())
                        .sideEffect(
                                __.V(entity2).
                                        outE("associated_with").
                                        inV().hasId(entity4).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("associated_with").
                                                        property("first_seen", 0L).
                                                        property("last_seen", 0L).
                                                        from(
                                                                __.V(entity2)).
                                                        to(
                                                                __.V(entity4))))
                        .sideEffect(
                                __.V(entity2).
                                        outE("associated_with").
                                        as("edge").
                                        inV().hasId(entity4).
                                        select("edge").
                                        coalesce(
                                                __.properties("internal dataset").drop(),
                                                __.property("last_seen", 0L)))
                        .sideEffect(
                                __.V(entity4).
                                        outE("associated_with").
                                        inV().hasId(entity2).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("associated_with").
                                                        property("first_seen", 0L).
                                                        property("last_seen", 0L).
                                                        from(
                                                                __.V(entity4)).
                                                        to(
                                                                __.V(entity2))))
                        .sideEffect(
                                __.V(entity4).
                                        outE("associated_with").
                                        as("edge").
                                        inV().hasId(entity2).
                                        select("edge").
                                        coalesce(
                                                __.properties("internal dataset").drop(),
                                                __.property("last_seen", 0L))).
                        toList();
        bh.consume(result);
    }

    @Benchmark
    public void benchmark_g_V_traversal_optimized_1(final Blackhole blackhole) {
        List<Vertex> vertexList =
                g.V(id1).fold().
                        coalesce(
                                __.<Vertex>unfold().sideEffect(
                                        __.coalesce(
                                                __.properties("internal dataset").drop(),
                                                __.property("last_seen", 0L))),
                                __.addV("entity_link").
                                        property(T.id, id1).
                                        property("type", "id").
                                        property("opt_ind", "0").
                                        property("first_seen", 0L).
                                        property("last_seen", 0L))
                        .sideEffect(
                                __.V(id4).fold().
                                        coalesce(
                                                __.<Vertex>unfold().sideEffect(
                                                        __.coalesce(
                                                                __.properties("internal dataset").drop(),
                                                                __.property("last_seen", 0L))),
                                                __.addV("ccsid_e").
                                                        property(T.id, id4).
                                                        property("type", "id").
                                                        property("opt_ind", "0").
                                                        property("first_seen", 0L).
                                                        property("last_seen", 0L)))
                        .sideEffect(
                                __.V(id6).fold().
                                        coalesce(
                                                __.<Vertex>unfold().sideEffect(
                                                        __.coalesce(
                                                                __.properties("internal dataset").drop(),
                                                                __.property("last_seen", 0L))),
                                                __.addV("dl_tid_e").
                                                        property(T.id, id6).
                                                        property("type", "id").
                                                        property("opt_ind", "0").
                                                        property("first_seen", 0L).
                                                        property("last_seen", 0L)))
                        .sideEffect(
                                __.V(entity2).fold().
                                        coalesce(
                                                __.<Vertex>unfold().sideEffect(
                                                        __.coalesce(
                                                                __.properties("internal dataset").drop())),
                                                __.addV("address").
                                                        property(T.id, entity2).
                                                        property("type", "entity").
                                                        property("first_seen", 0L)))
                        .sideEffect(
                                __.V(entity2).
                                        inE("identifies").
                                        outV().
                                        hasId(id1).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("identifies").
                                                        property("first_seen", 0L).
                                                        from(__.V(id1)).
                                                        to(__.V(entity2))))
                        .sideEffect(
                                __.V(entity2).
                                        inE("identifies").
                                        where(__.outV().hasId(id1)).
                                        properties("internal dataset").drop())
                        .sideEffect(
                                __.V(entity4).fold().
                                        coalesce(
                                                __.<Vertex>unfold().sideEffect(
                                                        __.coalesce(
                                                                __.properties("internal dataset").drop())),
                                                __.addV("sub_account").
                                                        property(T.id, entity4).
                                                        property("type", "entity").
                                                        property("first_seen", 0L)))
                        .sideEffect(
                                __.V(entity4).
                                        inE("identifies").
                                        outV().
                                        hasId(id4).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("identifies").
                                                        property("first_seen", 0L).
                                                        from(
                                                                __.V(id4)).
                                                        to(
                                                                __.V(entity4))))
                        .sideEffect(
                                __.V(entity4).
                                        inE("identifies").
                                        where(__.outV().hasId(id4)).
                                        properties("internal dataset").drop())
                        .sideEffect(
                                __.V(entity4).
                                        inE("identifies").
                                        outV().
                                        hasId(id6).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("identifies").
                                                        property("first_seen", 0L).
                                                        from(
                                                                __.V(id6)).
                                                        to(
                                                                __.V(entity4))))
                        .sideEffect(
                                __.V(entity4).
                                        inE("identifies").
                                        where(__.outV().hasId(id6)).
                                        properties("internal dataset").drop())
                        .sideEffect(
                                __.V(entity2).
                                        outE("associated_with").
                                        inV().
                                        hasId(entity4).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("associated_with").
                                                        property("first_seen", 0L).
                                                        property("last_seen", 0L).
                                                        from(__.V(entity2)).
                                                        to(__.V(entity4))))
                        .sideEffect(
                                __.V(entity2).
                                        outE("associated_with").
                                        where(__.inV().hasId(entity4)).
                                        coalesce(
                                                __.properties("internal dataset").drop(),
                                                __.property("last_seen", 0L)))
                        .sideEffect(
                                __.V(entity4).
                                        outE("associated_with").
                                        inV().
                                        hasId(entity2).
                                        fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("associated_with").
                                                        property("first_seen", 0L).
                                                        property("last_seen", 0L).
                                                        from(
                                                                __.V(entity4)).
                                                        to(
                                                                __.V(entity2))))
                        .sideEffect(
                                __.V(entity4).
                                        outE("associated_with").
                                        where(__.inV().hasId(entity2)).
                                        coalesce(
                                                __.properties("internal dataset").drop(),
                                                __.property("last_seen", 0L))).
                        toList();
        blackhole.consume(vertexList);
    }

    @Benchmark
    public void benchmark_g_V_traversal_extra_optimized_0(final Blackhole blackhole) {
        List<Vertex> vertexList =
                g.V(id1).fold().
                        coalesce(
                                __.<Vertex>unfold().sideEffect(
                                        __.coalesce(
                                                __.properties("internal dataset").drop(),
                                                __.property("last_seen", 0L))),
                                __.addV("entity_link").property(T.id, id1).
                                        property("type", "id").
                                        property("opt_ind", "0").
                                        property("first_seen", 0L).
                                        property("last_seen", 0L)).
                        sideEffect(__.V(entity2).fold().
                                coalesce(
                                        __.<Vertex>unfold().sideEffect(
                                                __.properties("internal dataset").drop()),
                                        __.addV("address").
                                                property(T.id, entity2).
                                                property("type", "entity").
                                                property("first_seen", 0L)).
                                sideEffect(__.in("identifies").hasId(id1).fold().
                                        coalesce(
                                                __.unfold(),
                                                __.addE("identifies").
                                                        property("first_seen", 0L).
                                                        from(__.V(id1)).
                                                        to(__.V(entity2)))).
                                sideEffect(
                                        __.inE("identifies").where(__.outV().hasId(entity2)).
                                                properties("internal dataset").
                                                drop())).toList();
        blackhole.consume(vertexList);
    }

    @Benchmark
    public void benchmark_g_V_traversal_unoptimized_2(final Blackhole blackhole) {
        g.V(id1).
                fold().
                coalesce(
                        __.unfold(),
                        __.addV("entity_link").
                                property(T.id, id1).
                                property("type", "id").
                                property("opt_ind", "0").
                                property("first_seen", 0L).
                                property("last_seen", 0L))
                .sideEffect(
                        __.V(id1).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(id2).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("base_cin_e").
                                                property(T.id, id2).
                                                property("type", "id").
                                                property("opt_ind", "0").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L)))
                .sideEffect(
                        __.V(id2).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(id3).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("cam_gid_e").
                                                property(T.id, id3).
                                                property("type", "id").
                                                property("opt_ind", "0").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L)))
                .sideEffect(
                        __.V(id3).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(id4).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("ccsid_e").
                                                property(T.id, id4).
                                                property("type", "id").
                                                property("opt_ind", "0").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L)))
                .sideEffect(
                        __.V(id4).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(id5).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("cl_tid_e").
                                                property(T.id, id5).
                                                property("type", "id").
                                                property("opt_ind", "0").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L)))
                .sideEffect(
                        __.V(id5).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(id6).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("dl_tid_e").
                                                property(T.id, id6).
                                                property("type", "id").
                                                property("opt_ind", "0").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L)))
                .sideEffect(
                        __.V(id6).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(id7).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("idl").
                                                property(T.id, id7).
                                                property("type", "id").
                                                property("opt_ind", "0").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L)))
                .sideEffect(
                        __.V(id7).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(id8).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("acctnum_e").
                                                property(T.id, id8).
                                                property("type", "id").
                                                property("opt_ind", "0").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L)))
                .sideEffect(
                        __.V(id8).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(id9).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("ccid_e").
                                                property(T.id, id9).
                                                property("type", "id").
                                                property("opt_ind", "0").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L)))
                .sideEffect(
                        __.V(id9).
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity1).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("account").
                                                property(T.id, entity1).
                                                property("type", "entity").
                                                property("first_seen", 0L)))
                .sideEffect(
                        __.V(entity1).
                                coalesce(
                                        __.properties("internal dataset").drop()))
                .sideEffect(
                        __.V(entity1).
                                inE("identifies").
                                outV().
                                hasId(id2).
                                fold().
                                coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id2)).
                                                to(__.V(entity1))))
                .sideEffect(
                        __.V(entity1).
                                inE("identifies").
                                as("edge").
                                outV().
                                hasId(id2).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity1).
                                inE("identifies").
                                outV().
                                hasId(id4).
                                fold().
                                coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id4)).
                                                to(__.V(entity1))))
                .sideEffect(
                        __.V(entity1).
                                inE("identifies").
                                as("edge").
                                outV().
                                hasId(id4).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity1).
                                inE("identifies").
                                outV().
                                hasId(id5).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id5)).
                                                to(__.V(entity1))))
                .sideEffect(
                        __.V(entity1).
                                inE("identifies").
                                as("edge").
                                outV().
                                hasId(id5).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity1).
                                inE("identifies").
                                outV().
                                hasId(id8).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id8)).
                                                to(__.V(entity1))))
                .sideEffect(
                        __.V(entity1).
                                inE("identifies").
                                as("edge").
                                outV().
                                hasId(id8).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity2).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("address").
                                                property(T.id, entity2).
                                                property("type", "entity").
                                                property("first_seen", 0L)))
                .sideEffect(
                        __.V(entity2).
                                coalesce(
                                        __.properties("internal dataset").drop()))
                .sideEffect(
                        __.V(entity2).
                                inE("identifies").
                                outV().
                                hasId(id1).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id1)).
                                                to(__.V(entity2))))
                .sideEffect(
                        __.V(entity2).
                                inE("identifies").
                                as("edge").
                                outV().
                                hasId(id1).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity3).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("individual").
                                                property(T.id, entity3).
                                                property("type", "entity").
                                                property("first_seen", 0L)))
                .sideEffect(
                        __.V(entity3).
                                coalesce(
                                        __.properties("internal dataset").drop()))
                .sideEffect(
                        __.V(entity3).
                                inE("identifies").
                                outV().
                                hasId(id3).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id3)).
                                                to(__.V(entity3))))
                .sideEffect(
                        __.V(entity3).
                                inE("identifies").
                                as("edge").
                                outV().
                                hasId(id3).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity3).
                                inE("identifies").
                                outV().
                                hasId(id4).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id4)).
                                                to(__.V(entity3))))
                .sideEffect(
                        __.V(entity3).
                                inE("identifies").
                                as("edge").
                                outV().
                                hasId(id4).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity3).
                                inE("identifies").
                                outV().
                                hasId(id7).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id7)).
                                                to(__.V(entity3))))
                .sideEffect(
                        __.V(entity3).
                                inE("identifies").
                                as("edge").
                                outV().
                                hasId(id7).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity3).
                                inE("identifies").
                                outV().
                                hasId(id9).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id9)).
                                                to(__.V(entity3))))
                .sideEffect(
                        __.V(entity3).
                                inE("identifies").as("edge").
                                outV().
                                hasId(id9).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity4).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addV("sub_account").
                                                property(T.id, entity4).
                                                property("type", "entity").
                                                property("first_seen", 0L)))
                .sideEffect(
                        __.V(entity4).
                                coalesce(
                                        __.properties("internal dataset").drop()))
                .sideEffect(
                        __.V(entity4).
                                inE("identifies").
                                outV().
                                hasId(id6).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("identifies").
                                                property("first_seen", 0L).
                                                from(__.V(id6)).
                                                to(__.V(entity4))))
                .sideEffect(
                        __.V(entity4).
                                inE("identifies").
                                as("edge").
                                outV().
                                hasId(id6).
                                select("edge").
                                properties("internal dataset").drop())
                .sideEffect(
                        __.V(entity1).
                                outE("associated_with").
                                inV().
                                hasId(entity2).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity1)).
                                                to(__.V(entity2))))
                .sideEffect(
                        __.V(entity1).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity2).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity1).
                                outE("associated_with").
                                inV().hasId(entity3).
                                fold().
                                coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity1)).
                                                to(__.V(entity3))))
                .sideEffect(
                        __.V(entity1).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity3).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity1).
                                outE("associated_with").
                                inV().
                                hasId(entity4).
                                fold().
                                coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity1)).
                                                to(__.V(entity4))))
                .sideEffect(
                        __.V(entity1).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity4).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity2).
                                outE("associated_with").
                                inV().
                                hasId(entity1).
                                fold().
                                coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity2)).
                                                to(__.V(entity1))))
                .sideEffect(
                        __.V(entity2).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity1).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity2).
                                outE("associated_with").
                                inV().
                                hasId(entity3).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity2)).
                                                to(__.V(entity3))))
                .sideEffect(
                        __.V(entity2).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity3).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity2).
                                outE("associated_with").
                                inV().
                                hasId(entity4).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity2)).
                                                to(__.V(entity4))))
                .sideEffect(
                        __.V(entity2).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity4).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity3).
                                outE("associated_with").
                                inV().
                                hasId(entity1).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity3)).
                                                to(__.V(entity1))))
                .sideEffect(
                        __.V(entity3).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity1).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity3).
                                outE("associated_with").
                                inV().
                                hasId(entity2).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").

                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity3)).
                                                to(__.V(entity2))))
                .sideEffect(
                        __.V(entity3).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity2).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity3).
                                outE("associated_with").
                                inV().
                                hasId(entity4).
                                fold().
                                coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity3)).
                                                to(__.V(entity4))))
                .sideEffect(
                        __.V(entity3).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity4).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity4).
                                outE("associated_with").
                                inV().
                                hasId(entity1).
                                fold().
                                coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity4)).
                                                to(__.V(entity1))))
                .sideEffect(
                        __.V(entity4).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity1).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity4).
                                outE("associated_with").
                                inV().
                                hasId(entity2).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity4)).
                                                to(__.V(entity2))))
                .sideEffect(
                        __.V(entity4).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity2).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L)))
                .sideEffect(
                        __.V(entity4).
                                outE("associated_with").
                                inV().
                                hasId(entity3).
                                fold().coalesce(
                                        __.unfold(),
                                        __.addE("associated_with").
                                                property("first_seen", 0L).
                                                property("last_seen", 0L).
                                                from(__.V(entity4)).
                                                to(__.V(entity3))))
                .sideEffect(
                        __.V(entity4).
                                outE("associated_with").
                                as("edge").
                                inV().
                                hasId(entity3).
                                select("edge").
                                coalesce(
                                        __.properties("internal dataset").drop(),
                                        __.property("last_seen", 0L))).iterate();
    }
}
