package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadSampleLimitStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadSampleLimitStep;
import com.aerospike.firefly.process.traversal.step.FireflyOtherVBatchReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyEdgeToVertexBatchReadStep;
import com.aerospike.firefly.process.traversal.step.map.FireflyAdjacentVertexIdStep;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyMixedStrategyTest {
    private static FireflyGraph graph;
    private static Client client;
    private static Cluster cluster;

    private static final List<Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>>> V_ID_FUZZY_STRATEGY = List.of(
            new Pair<>(
                    List.of(FireflyAdjacentVertexIdStep.class),
                    t -> t.id()
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadStep.class, IdStep.class),
                    t -> t.has("name", "foo").id()
            ),
            new Pair<>(
                    List.of(FireflyAdjacentVertexIdStep.class, RangeGlobalStep.class),
                    t -> t.limit(5).id()
            ),
            new Pair<>(
                    List.of(FireflyAdjacentVertexIdStep.class, SampleGlobalStep.class),
                    t -> t.sample(5).id()
            ),
            new Pair<>(
                    List.of(FireflyAdjacentVertexIdStep.class, HasStep.class),
                    t -> t.id().has("name", "foo")
            ),
            new Pair<>(
                    List.of(FireflyAdjacentVertexIdStep.class, RangeGlobalStep.class),
                    t -> t.id().limit(5)
            ),
            new Pair<>(
                    List.of(FireflyAdjacentVertexIdStep.class, SampleGlobalStep.class),
                    t -> t.id().sample(5)
            )
    );

    private static final List<Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>>> OTHERV_FUZZY_STRATEGY = List.of(
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, RangeGlobalStep.class, SampleGlobalStep.class),
                    t -> t.has("name", "test").limit(10).sample(5)
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, RangeGlobalStep.class),
                    t -> t.has("name", "test").limit(10)
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, SampleGlobalStep.class),
                    t -> t.has("name", "test").sample(5)
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class),
                    t -> t.has("status", "active")
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, RangeGlobalStep.class, HasStep.class, SampleGlobalStep.class),
                    t -> {
                        t.limit(15);
                        t.has("type", "sensor");
                        t.sample(3);
                    }
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, RangeGlobalStep.class, SampleGlobalStep.class),
                    t -> t.limit(10).sample(5)
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, RangeGlobalStep.class),
                    t -> t.limit(10)
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, SampleGlobalStep.class),
                    t -> t.sample(5)
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, SampleGlobalStep.class, RangeGlobalStep.class),
                    t -> {
                        t.sample(5);
                        t.limit(10);
                    }
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, RangeGlobalStep.class, HasStep.class),
                    t -> {
                        t.limit(10);
                        t.has("category", "device");
                    }
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, SampleGlobalStep.class, HasStep.class, RangeGlobalStep.class),
                    t -> {
                        t.sample(5);
                        t.has("sensor", true);
                        t.limit(10);
                    }
            ),
            new Pair<>(
                    List.of(FireflyOtherVBatchReadStep.class, SampleGlobalStep.class, RangeGlobalStep.class, HasStep.class),
                    t -> {
                        t.sample(5);
                        t.limit(15);
                        t.has("device", "A");
                    }
            )
    );
    // TODO: OtherV()/hasId()/inV()/outV()/??
    private static final List<Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>>> INV_OUTV_BOTHV_FUZZY_STRATEGY = List.of(
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, RangeGlobalStep.class, SampleGlobalStep.class),
                    t -> t.has("name", "test").limit(10).sample(5)
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, RangeGlobalStep.class),
                    t -> t.has("name", "test").limit(10)
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, SampleGlobalStep.class),
                    t -> t.has("name", "test").sample(5)
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class),
                    t -> t.has("status", "active")
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, RangeGlobalStep.class, HasStep.class, SampleGlobalStep.class),
                    t -> {
                        t.limit(15);
                        t.has("type", "sensor");
                        t.sample(3);
                    }
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, RangeGlobalStep.class, SampleGlobalStep.class),
                    t -> t.limit(10).sample(5)
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, RangeGlobalStep.class),
                    t -> t.limit(10)
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, SampleGlobalStep.class),
                    t -> t.sample(5)
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, SampleGlobalStep.class, RangeGlobalStep.class),
                    t -> {
                        t.sample(5);
                        t.limit(10);
                    }
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, RangeGlobalStep.class, HasStep.class),
                    t -> {
                        t.limit(10);
                        t.has("category", "device");
                    }
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, SampleGlobalStep.class, HasStep.class, RangeGlobalStep.class),
                    t -> {
                        t.sample(5);
                        t.has("sensor", true);
                        t.limit(10);
                    }
            ),
            new Pair<>(
                    List.of(FireflyEdgeToVertexBatchReadStep.class, SampleGlobalStep.class, RangeGlobalStep.class, HasStep.class),
                    t -> {
                        t.sample(5);
                        t.limit(15);
                        t.has("device", "A");
                    }
            )
    );

    private static final List<Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>>> EDGE_FUZZY_STRATEGY = List.of(
            new Pair<>(
                    List.of(FireflyBatchEdgeReadStep.class, RangeGlobalStep.class, SampleGlobalStep.class),
                    t -> t.has("name", "test").limit(10).sample(5)
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadStep.class, RangeGlobalStep.class),
                    t -> t.has("name", "test").limit(10)
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadStep.class, SampleGlobalStep.class, RangeGlobalStep.class),
                    t -> t.has("name", "test").sample(5)
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadStep.class),
                    t -> t.has("status", "active")
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadSampleLimitStep.class, RangeGlobalStep.class, HasStep.class, SampleGlobalStep.class),
                    t -> {
                        t.limit(15);
                        t.has("type", "sensor");
                        t.sample(3);
                    }
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadSampleLimitStep.class, RangeGlobalStep.class, SampleGlobalStep.class),
                    t -> t.limit(10).sample(5)
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadSampleLimitStep.class, RangeGlobalStep.class),
                    t -> t.limit(10)
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadSampleLimitStep.class, RangeGlobalStep.class),
                    t -> t.sample(5)
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadSampleLimitStep.class, RangeGlobalStep.class, RangeGlobalStep.class),
                    t -> {
                        t.sample(5);
                        t.limit(10);
                    }
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadSampleLimitStep.class, RangeGlobalStep.class, HasStep.class),
                    t -> {
                        t.limit(10);
                        t.has("category", "device");
                    }
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadSampleLimitStep.class, RangeGlobalStep.class, HasStep.class, RangeGlobalStep.class),
                    t -> {
                        t.sample(5);
                        t.has("sensor", true);
                        t.limit(10);
                    }
            ),
            new Pair<>(
                    List.of(FireflyBatchEdgeReadSampleLimitStep.class, RangeGlobalStep.class, RangeGlobalStep.class, HasStep.class),
                    t -> {
                        t.sample(5);
                        t.limit(15);
                        t.has("device", "A");
                    }
            )
    );

    private static final List<Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>>> VERTEX_FUZZY_STRATEGY = List.of(
            new Pair<>(
                    List.of(FireflyBatchVertexReadStep.class, RangeGlobalStep.class, SampleGlobalStep.class),
                    t -> t.has("name", "test").limit(10).sample(5)
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadStep.class, RangeGlobalStep.class),
                    t -> t.has("name", "test").limit(10)
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadStep.class, SampleGlobalStep.class),
                    t -> t.has("name", "test").sample(5)
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadStep.class),
                    t -> t.has("status", "active")
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadSampleLimitStep.class, RangeGlobalStep.class, HasStep.class, SampleGlobalStep.class),
                    t -> {
                        t.limit(15);
                        t.has("type", "sensor");
                        t.sample(3);
                    }
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadSampleLimitStep.class, RangeGlobalStep.class, SampleGlobalStep.class),
                    t -> t.limit(10).sample(5)
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadSampleLimitStep.class, RangeGlobalStep.class),
                    t -> t.limit(10)
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadSampleLimitStep.class, RangeGlobalStep.class),
                    t -> t.sample(5)
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadSampleLimitStep.class, RangeGlobalStep.class, RangeGlobalStep.class),
                    t -> {
                        t.sample(5);
                        t.limit(10);
                    }
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadSampleLimitStep.class, RangeGlobalStep.class, HasStep.class),
                    t -> {
                        t.limit(10);
                        t.has("category", "device");
                    }
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadSampleLimitStep.class, RangeGlobalStep.class, HasStep.class, RangeGlobalStep.class),
                    t -> {
                        t.sample(5);
                        t.has("sensor", true);
                        t.limit(10);
                    }
            ),
            new Pair<>(
                    List.of(FireflyBatchVertexReadSampleLimitStep.class, RangeGlobalStep.class, RangeGlobalStep.class, HasStep.class),
                    t -> {
                        t.sample(5);
                        t.limit(15);
                        t.has("device", "A");
                    }
            )
    );

    public static class Pair<A, B> {
        public final A first;
        public final B second;

        public Pair(A first, B second) {
            this.first = first;
            this.second = second;
        }
    }

    @BeforeClass
    public static void setup() {
        graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));
        cluster = Cluster.build()
                .addContactPoint("localhost").port(8182)
                .create();
        client = cluster.connect();
    }

    @AfterClass
    public static void tearDown() {
        if (client != null) {
            client.close();
            client = null;
        }
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
        if (graph != null) {
            graph.close();
            graph = null;
        }
    }

    @Test
    public void testStrategyFuzzingVertexStepHasStep() {
        final Random random = new Random();
        for (int i = 0; i < 100; i++) {
            final GraphTraversal t = graph.traversal().V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(2);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = VERTEX_FUZZY_STRATEGY.get(random.nextInt(VERTEX_FUZZY_STRATEGY.size()));
                switch (random.nextInt(3)) {
                    case 0:
                        t.in();
                        break;
                    case 1:
                        t.out();
                        break;
                    case 2:
                        t.both();
                        break;
                }
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
        for (int i = 0; i < 100; i++) {
            final GraphTraversal t = graph.traversal().V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(5);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = VERTEX_FUZZY_STRATEGY.get(random.nextInt(VERTEX_FUZZY_STRATEGY.size()));
                switch (random.nextInt(3)) {
                    case 0:
                        t.in();
                        break;
                    case 1:
                        t.out();
                        break;
                    case 2:
                        t.both();
                        break;
                }
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
    }

    @Test
    public void testStrategyVertexId() {
        System.out.println(graph.traversal().V().out().id().limit(5).explain());
        final Random random = new Random();
        for (int i = 0; i < 100; i++) {
            final GraphTraversal t = graph.traversal().V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(2);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = V_ID_FUZZY_STRATEGY.get(random.nextInt(V_ID_FUZZY_STRATEGY.size()));
                switch (random.nextInt(3)) {
                    case 0:
                        t.in();
                        break;
                    case 1:
                        t.out();
                        break;
                    case 2:
                        t.both();
                        break;
                }
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
        for (int i = 0; i < 100; i++) {
            final GraphTraversal t = graph.traversal().V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(5);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = V_ID_FUZZY_STRATEGY.get(random.nextInt(V_ID_FUZZY_STRATEGY.size()));
                switch (random.nextInt(3)) {
                    case 0:
                        t.in();
                        break;
                    case 1:
                        t.out();
                        break;
                    case 2:
                        t.both();
                        break;
                }
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
    }

    @Test
    public void testStrategyFuzzingInVOutVBothVHasStep() {
        final Random random = new Random();
        for (int i = 0; i < 100; i++) {
            final GraphTraversal t = graph.traversal().V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(2);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = INV_OUTV_BOTHV_FUZZY_STRATEGY.get(random.nextInt(INV_OUTV_BOTHV_FUZZY_STRATEGY.size()));
                switch (random.nextInt(3)) {
                    case 0:
                        t.inV();
                        break;
                    case 1:
                        t.outV();
                        break;
                    case 2:
                        t.bothV();
                        break;
                }
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
        for (int i = 0; i < 100; i++) {
            final GraphTraversal t = graph.traversal().V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(5);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = INV_OUTV_BOTHV_FUZZY_STRATEGY.get(random.nextInt(INV_OUTV_BOTHV_FUZZY_STRATEGY.size()));
                switch (random.nextInt(3)) {
                    case 0:
                        t.inV();
                        break;
                    case 1:
                        t.outV();
                        break;
                    case 2:
                        t.bothV();
                        break;
                }
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
    }

    @Test
    public void testStrategyFuzzingOtherVHasStep() {
        final Random random = new Random();
        for (int i = 0; i < 100; i++) {
            // Early limit switches g.V().otherV().limit(5) to g.V().limit(5).otherV() which causes our predictiosn to be wrong, even tho it is an invalid query anyway...
            final GraphTraversal t = graph.traversal().withoutStrategies(EarlyLimitStrategy.class).V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(2);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = OTHERV_FUZZY_STRATEGY.get(random.nextInt(OTHERV_FUZZY_STRATEGY.size()));
                t.otherV();
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
        for (int i = 0; i < 100; i++) {
            // Early limit switches g.V().otherV().limit(5) to g.V().limit(5).otherV() which causes our predictiosn to be wrong, even tho it is an invalid query anyway...
            final GraphTraversal t = graph.traversal().withoutStrategies(EarlyLimitStrategy.class).V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(5);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = OTHERV_FUZZY_STRATEGY.get(random.nextInt(OTHERV_FUZZY_STRATEGY.size()));
                t.otherV();
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
    }

    @Test
    public void testStrategyFuzzingEdgeStepHasStep() {
        final Random random = new Random();
        for (int i = 0; i < 100; i++) {
            final GraphTraversal t = graph.traversal().V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(2);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = EDGE_FUZZY_STRATEGY.get(random.nextInt(VERTEX_FUZZY_STRATEGY.size()));
                switch (random.nextInt(3)) {
                    case 0:
                        t.inE();
                        break;
                    case 1:
                        t.outE();
                        break;
                    case 2:
                        t.bothE();
                        break;
                }
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
        for (int i = 0; i < 100; i++) {
            final GraphTraversal t = graph.traversal().V();
            final List<Class> expectSteps = new ArrayList<>();
            expectSteps.add(FireflyGraphStep.class);
            final int stepsToAdd = random.nextInt(5);
            for (int j = 0; j < stepsToAdd; j++) {
                final Pair<List<Class<?>>, Consumer<GraphTraversal<?, ?>>> pair = EDGE_FUZZY_STRATEGY.get(random.nextInt(VERTEX_FUZZY_STRATEGY.size()));
                switch (random.nextInt(3)) {
                    case 0:
                        t.inE();
                        break;
                    case 1:
                        t.outE();
                        break;
                    case 2:
                        t.bothE();
                        break;
                }
                expectSteps.addAll(pair.first);
                pair.second.accept(t);
            }
            final List<Step> pureSteps = t.asAdmin().clone().getSteps();
            t.asAdmin().applyStrategies();
            final List<Step> steps = t.asAdmin().getSteps();
            if (steps.size() != expectSteps.size()) {
                reportError(steps, expectSteps, pureSteps);
            }
            for (int j = 0; j < steps.size(); j++) {
                if (!steps.get(j).getClass().equals(expectSteps.get(j))) {
                    reportError(steps, expectSteps, pureSteps);
                }
            }
        }
    }

    private void reportError(final List<Step> actualSteps,
                             final List<Class> expectedSteps,
                             final List<Step> pureSteps) {
        StringBuilder errorMessage = new StringBuilder("Expected steps: ");
        for (Class stepClass : expectedSteps) {
            errorMessage.append(stepClass.getSimpleName()).append(", ");
        }
        errorMessage.append("\nActual steps: ");
        for (Step step : actualSteps) {
            errorMessage.append(step.getClass().getSimpleName()).append(", ");
        }
        errorMessage.append("\nPure steps: ");
        for (Step step : pureSteps) {
            errorMessage.append(step.getClass().getSimpleName()).append(", ");
        }
        throw new AssertionError(errorMessage.toString());
    }

    @Test
    public void testMultipleIn() {
        final Traversal.Admin traversal = graph.traversal().V().in().in().in().in().in().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleOut() {
        final Traversal.Admin traversal = graph.traversal().V().out().out().out().out().out().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleBoth() {
        final Traversal.Admin traversal = graph.traversal().V().both().both().both().both().both().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleInOut() {
        final Traversal.Admin traversal = graph.traversal().V().in().out().in().out().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleOutIn() {
        final Traversal.Admin traversal = graph.traversal().V().out().in().out().in().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleBothIn() {
        final Traversal.Admin traversal = graph.traversal().V().both().in().both().in().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleBothOut() {
        final Traversal.Admin traversal = graph.traversal().V().both().out().both().out().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleInBoth() {
        final Traversal.Admin traversal = graph.traversal().V().in().both().in().both().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleOutBoth() {
        final Traversal.Admin traversal = graph.traversal().V().out().both().out().both().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleInHas() {
        final Traversal.Admin traversal = graph.traversal().V().in().has("name", "test").in().
                in().has("name", "test").in().has("name", "test").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleOutHas() {
        final Traversal.Admin traversal = graph.traversal().V().out().has("name", "test").out().
                out().has("name", "test").out().has("name", "test").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleBothHas() {
        final Traversal.Admin traversal = graph.traversal().V().both().has("name", "test").both().
                both().has("name", "test").both().has("name", "test").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleInHasId() {
        final Traversal.Admin traversal = graph.traversal().V().in().hasId("test").in().
                in().hasId("test").in().hasId("test").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleOutHasId() {
        final Traversal.Admin traversal = graph.traversal().V().out().hasId("test").out().
                out().hasId("test").out().hasId("test").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleBothHasId() {
        final Traversal.Admin traversal = graph.traversal().V().both().hasId("test").both().
                both().hasId("test").both().hasId("test").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleInHasLabel() {
        final Traversal.Admin traversal = graph.traversal().V().in().hasLabel("test").in().
                in().hasLabel("test").in().hasLabel("test").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleOutHasLabel() {
        final Traversal.Admin traversal = graph.traversal().V().out().hasLabel("test").out().
                out().hasLabel("test").out().hasLabel("test").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleBothHasLabel() {
        final Traversal.Admin traversal = graph.traversal().V().both().hasLabel("test").both().
                both().hasLabel("test").both().hasLabel("test").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleInHasAs() {
        final Traversal.Admin traversal = graph.traversal().V().in().has("name", "test").as("a").in().
                in().has("name", "test").as("b").in().has("name", "test").as("c").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleOutHasAs() {
        final Traversal.Admin traversal = graph.traversal().V().out().has("name", "test").as("a").out().
                out().has("name", "test").as("b").out().has("name", "test").as("c").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleBothHasAs() {
        final Traversal.Admin traversal = graph.traversal().V().both().has("name", "test").as("a").both().
                both().has("name", "test").as("b").both().has("name", "test").as("c").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleInHasIdAs() {
        final Traversal.Admin traversal = graph.traversal().V().in().hasId("test").as("a").in().
                in().hasId("test").as("b").in().hasId("test").as("c").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleOutHasIdAs() {
        final Traversal.Admin traversal = graph.traversal().V().out().hasId("test").as("a").out().
                out().hasId("test").as("b").out().hasId("test").as("c").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void testMultipleBothHasIdAs() {
        final Traversal.Admin traversal = graph.traversal().V().both().hasId("test").as("a").both().
                both().hasId("test").as("b").both().hasId("test").as("c").asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class));
    }

    @Test
    public void test2InId() {
        final Traversal.Admin traversal = graph.traversal().V().in().in().id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyAdjacentVertexIdStep.class));
    }

    @Test
    public void test3InId() {
        final Traversal.Admin traversal = graph.traversal().V().in().in().in().id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyAdjacentVertexIdStep.class));
    }

    @Test
    public void test4InId() {
        final Traversal.Admin traversal = graph.traversal().V().in().in().in().in().id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyAdjacentVertexIdStep.class));
    }

    @Test
    public void test2OutId() {
        final Traversal.Admin traversal = graph.traversal().V().out().out().id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyAdjacentVertexIdStep.class));
    }

    @Test
    public void test3OutId() {
        final Traversal.Admin traversal = graph.traversal().V().out().out().out().id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyAdjacentVertexIdStep.class));
    }

    @Test
    public void test4OutId() {
        final Traversal.Admin traversal = graph.traversal().V().out().out().out().out().id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyAdjacentVertexIdStep.class));
    }

    @Test
    public void test2BothId() {
        final Traversal.Admin traversal = graph.traversal().V().both().both().id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyAdjacentVertexIdStep.class));
    }

    @Test
    public void test3BothId() {
        final Traversal.Admin traversal = graph.traversal().V().both().both().both().id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyAdjacentVertexIdStep.class));
    }

    @Test
    public void test4BothId() {
        final Traversal.Admin traversal = graph.traversal().V().both().both().both().both().id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, FireflyAdjacentVertexIdStep.class));
    }

    @Test
    public void test2InIdHas() {
        final Traversal.Admin traversal = graph.traversal().V().in().in().has("name", "test").id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, IdStep.class));
    }

    @Test
    public void test3InIdHas() {
        final Traversal.Admin traversal = graph.traversal().V().in().in().in().has("name", "test").id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, IdStep.class));
    }

    @Test
    public void test2OutIdHas() {
        final Traversal.Admin traversal = graph.traversal().V().out().out().has("name", "test").id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, IdStep.class));
    }

    @Test
    public void test3OutIdHas() {
        final Traversal.Admin traversal = graph.traversal().V().out().out().out().has("name", "test").id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, IdStep.class));
    }

    @Test
    public void test2BothIdHas() {
        final Traversal.Admin traversal = graph.traversal().V().both().both().has("name", "test").id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, IdStep.class));
    }

    @Test
    public void test3BothIdHas() {
        final Traversal.Admin traversal = graph.traversal().V().both().both().both().has("name", "test").id().asAdmin();
        traversal.applyStrategies();
        listClassCompare(traversal.getSteps(), List.of(FireflyGraphStep.class, FireflyBatchVertexReadStep.class,
                FireflyBatchVertexReadStep.class, FireflyBatchVertexReadStep.class, IdStep.class));
    }


    private void listClassCompare(final List<Step> actualSteps, final List<Class> expectedSteps) {
        if (actualSteps.size() != expectedSteps.size()) {
            throw new AssertionError("Size mismatch: " + actualSteps.size() + " != " + expectedSteps.size());
        }
        for (int i = 0; i < actualSteps.size(); i++) {
            Step actualStep = actualSteps.get(i);
            Class expectedStep = expectedSteps.get(i);
            if (!actualStep.getClass().equals(expectedStep)) {
                throw new AssertionError("Class mismatch at index " + i + ": " + actualStep.getClass() + " != " + expectedStep);
            }
        }
    }
}
