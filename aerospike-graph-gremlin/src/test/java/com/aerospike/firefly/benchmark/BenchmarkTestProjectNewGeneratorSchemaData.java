package com.aerospike.firefly.benchmark;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 1, time = 45, timeUnit = TimeUnit.SECONDS)
public class BenchmarkTestProjectNewGeneratorSchemaData {
    // Sample usage: mvn test -Dfirefly.host=172.17.0.3 -Dbenchmark.mode=[all|throughput|average] -Dbenchmark.threads=4 -Ddocker.benchmark=1 -Dtest=BenchmarkTestProjectNewGeneratorSchemaData -DfailIfNoTests=false --no-transfer-progress
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTestProjectNewGeneratorSchemaData.class);
    private static final String HOST = BenchmarkTestUtils.getHost();
    private static final int PORT = 8182;
    private static final Integer THREADS = BenchmarkTestUtils.getThreads();
    private static final Mode MODE = BenchmarkTestUtils.getMode(LOG);
    private Cluster cluster = null;
    private GraphTraversalSource g = null;

    private static class Schema {
        private static final Class<GoldenEntity> root = GoldenEntity.class;

        public static final class GoldenEntity {
            public static final String label = "goldenEntity";
            public static final String observedEdge = "observed";
            public static final String resolvesToIndividualEdge = "resolvesToIndividual";
            public static final String hasContactMediumEdge = "hasContactMedium";
            public static Set<String> inEdges = Set.of();
        }

        public static final class DigitalEntity {
            public static final String label = "digitalEntity";
            public static final String connectedFromIpEdge = "connectedFromIp";
            public static final String associatedWithCookieEdge = "associatedWithCookie";
            public static Set<String> inEdges = Set.of(GoldenEntity.observedEdge);

            public static class PropertyKeys {
                public static final String macAddress = "macAddress";
                public static final String make = "make";
                public static final String model = "model";
            }
        }

        public static final class Individual {
            public static final String label = "individual";
            public static final String assignedSSNEdge = "assignedSSN";
            public static final String livesAtEdge = "livesAt";
            public static Set<String> inEdges = Set.of(
                    GoldenEntity.resolvesToIndividualEdge);
        }

        public static final class Household {
            public static final String label = "household";
            public static Set<String> inEdges = Set.of(Individual.livesAtEdge);
        }

        public static final class IPAddress {
            public static final String label = "ipAddress";
            public static final String assignedIPEdge = "assignedIP";
            public static Set<String> inEdges = Set.of(
                    DigitalEntity.connectedFromIpEdge);
        }

        public static final class ContactMedium {
            public static final String label = "contactMedium";
            public static Set<String> inEdges = Set.of(GoldenEntity.hasContactMediumEdge);
        }

        public static final class Cookie {
            public static final String label = "cookie";
            public static Set<String> inEdges = Set.of(DigitalEntity.associatedWithCookieEdge);
        }
    }

    private final Set<String> ENTITY_LABELS = Set.of(
            "digitalEntity",
            "individual",
            "household",
            "ipAddress",
            "contactMedium",
            "cookie");
    private final Set<String> ID_LABELS = Set.of("cookie", "ipAddress");

    private final Map<String, List<Object>> labelToIds = new HashMap<>();
    private final Map<String, List<Object>> labelToEntity = new HashMap<>();
    private final List<Object> rootIds = new ArrayList<>();
    private final List<Object> householdIds = new ArrayList<>();

    private final Map<String, List<String>> labelToInELabels = new HashMap<>();
    private final Map<String, List<String>> labelToOutELabels = new HashMap<>();
    private final List<String> macAddressValues = new ArrayList<>();

    private final Random random = new Random();
    private static final Cluster.Builder BUILDER = Cluster.build().
            addContactPoint(HOST).
            port(PORT).
            enableSsl(false).
            maxConnectionPoolSize(THREADS).
            minConnectionPoolSize(THREADS);

    // Setup for the benchmark. Note, BeforeClass won"t work to set static variables
    // because jmh launches a separate JVM for the benchmark and the @BeforeClass
    // annotation is ignored.
    @Setup
    public void setup(final BenchmarkParams benchmarkParams) {
        LOG.info("Creating the Cluster (setup).");
        cluster = BUILDER.create();

        LOG.info("Creating the GraphTraversalSource (setup).");
        g = traversal().withRemote(DriverRemoteConnection.using(cluster));
        ENTITY_LABELS.forEach(label -> {
            final Vertex sample = g.V().hasLabel(label).next();
            labelToInELabels.put(label, g.V(sample).inE().label().dedup().toList());
            labelToOutELabels.put(label, g.V(sample).outE().label().dedup().toList());
        });
        g.V().hasLabel(Schema.Household.label).id().limit(5000).forEachRemaining(householdIds::add);

        g.V().hasLabel(Schema.GoldenEntity.label).id().limit(5000).forEachRemaining(rootIds::add);
        g.V().hasLabel(Schema.DigitalEntity.label).values(Schema.DigitalEntity.PropertyKeys.macAddress).limit(5000)
                .forEachRemaining(it -> macAddressValues.add(it.toString()));
        // Get id lists for entity labels.
        for (final String label : ENTITY_LABELS) {
            LOG.info("Retrieving {} ids (setup).", label);
            final List<Object> ids = g.V().hasLabel(label).id().limit(5000).toList();
            LOG.info("Adding {} ids to map (setup).", ids.size());
            labelToEntity.put(label, ids);
        }

        // Get id lists for id labels.
        for (final String label : ID_LABELS) {
            LOG.info("Retrieving {} ids (setup).", label);
            final List<Object> ids = g.V().hasLabel(label).id().limit(5000).toList();
            LOG.info("Adding {} ids to map (setup).", ids.size());
            labelToIds.put(label, ids);
        }
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
                .include(BenchmarkTestProjectNewGeneratorSchemaData.class.getSimpleName())
                .detectJvmArgs()
                .forks(4)
                .threads(THREADS)
                .mode(MODE)
                .measurementTime(TimeValue.seconds(15))
                .timeout(TimeValue.minutes(2)); // Timeout
        BenchmarkTestUtils.appendJmhOptionsBuilder(optBuilder);
        Options opt = optBuilder.build();
        Collection<RunResult> runResult = new Runner(opt).run();

        LOG.info("Results:");
        LOG.info(runResult.toString());

        final JSONArray root = new JSONArray();
        runResult.forEach(result -> {
            JSONObject obj = new JSONObject();
            obj.put("name", result.getPrimaryResult().getLabel());
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
        try (final FileWriter file = new FileWriter("target/benchmark.json")) {
            file.write(root.toString());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    public void directLookupProperties(final Blackhole blackhole) {
        final Object id = rootIds.stream().skip(random.nextInt(rootIds.size())).findFirst().get();
        final List<? extends Property<Object>> digitalEntityProperties = g.V(id).properties().toList();
    }

    @Benchmark
    public void oneHopToDigitalEntityGetProperties(final Blackhole blackhole) {
        final Object id = rootIds.stream().skip(random.nextInt(rootIds.size())).findFirst().get();
        final List<? extends Property<Object>> digitalEntityProperties = g.V(id).outE(Schema.GoldenEntity.observedEdge).otherV().properties().toList();
    }

    @Benchmark
    public void twoHopToCookieGetProperties(final Blackhole blackhole) {
        final Object id = rootIds.stream().skip(random.nextInt(rootIds.size())).findFirst().get();
        g.V(id)
                .out(Schema.GoldenEntity.observedEdge)
                .out(Schema.DigitalEntity.associatedWithCookieEdge)
                .properties().toList();
    }

    @Benchmark
    public void threeHopToDigitalEntity(final Blackhole blackhole) {
        final Object houseHoldId = householdIds.stream().skip(random.nextInt(householdIds.size())).findFirst().get();
        g.V(houseHoldId)
                .in(Schema.Individual.livesAtEdge)
                .in(Schema.GoldenEntity.resolvesToIndividualEdge)
                .out(Schema.GoldenEntity.observedEdge)
                .properties().toList();
    }

    @Benchmark
    public void testSearchByVertexPropertyValue(final Blackhole blackhole) {
        g.V().has(Schema.DigitalEntity.PropertyKeys.macAddress, macAddressValues.get(random.nextInt(macAddressValues.size()))).next();
    }

    @Benchmark
    public void addTwoVertexOneEdge(final Blackhole blackhole) {
        g.addV().addE("test").to(__.addV()).next();
    }

    @Benchmark
    public void twoHopSelectByLabelCountProperties(final Blackhole blackhole) {
        final Object id = rootIds.stream().skip(random.nextInt(rootIds.size())).findFirst().get();
        g.V(id)
                .outE(Schema.GoldenEntity.observedEdge)
                .otherV()
                .out()
                .hasLabel(Schema.Cookie.label)
                .properties().count().next();
    }

    @Benchmark
    public void countLabelsInSubgraphTwoHop(final Blackhole blackhole) {
        final Object id = rootIds.stream().skip(random.nextInt(rootIds.size())).findFirst().get();
        g.V(id).out().out().groupCount().by(T.label).next();
    }
}
