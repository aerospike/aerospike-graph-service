package com.aerospike.firefly.call;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.fail;

public class TestFireflySindexCallSteps {

    private static final int INSERT_COUNT = 200000;

    public static void insertPersons(final GraphTraversalSource g) {
        for (int i = 0; i < INSERT_COUNT; i++) {
            g.addV("person").property("nameA", "person" + i).next();
        }
    }

    public static void loadGraph(final GraphTraversalSource g) throws InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> insertPersons(g));
        }
        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.MINUTES);
    }

    @BeforeClass
    public static void setUp() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            loadGraph(g);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDown() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
        }
    }

    @Test
    public void testSindexCreateTwiceError() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            g.call("aerospike.graph.admin.index.drop").
                    with("property_key", "nameA").
                    with("element_type", "vertex").next();
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameA").
                    with("element_type", "vertex").next();
            try {
                g.call("aerospike.graph.admin.index.create").
                        with("property_key", "nameA").
                        with("element_type", "vertex").next();
                fail("Expected second call to create index to fail");
            } catch (final Exception ignored) {
            }
        }
    }

    @Test
    public void testList() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            fireflyGraph.getBaseGraph().dropGraphIndices(fireflyGraph);
            final List<String> indexesAfterDrop = (List<String>) g.call("aerospike.graph.admin.index.list").next();
            Assert.assertTrue(indexesAfterDrop.isEmpty());
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "~label").
                    with("element_type", "vertex").next();
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameB").
                    with("element_type", "vertex").next();
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameC").
                    with("element_type", "vertex").next();
            Map<String, Long> nameBStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "nameB").
                    with("element_type", "vertex").next();
            Map<String, Long> nameCStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "nameC").
                    with("element_type", "vertex").next();
            Map<String, Long> labelStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "~label").
                    with("element_type", "vertex").next();
            while (nameBStatus.get("percent_complete") < 100 ||
                    nameCStatus.get("percent_complete") < 100 ||
                    labelStatus.get("percent_complete") < 100) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                nameBStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "nameB").
                        with("element_type", "vertex").next();
                nameCStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "nameC").
                        with("element_type", "vertex").next();
                labelStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "~label").
                        with("element_type", "vertex").next();
            }
            final List<String> sindexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
            Assert.assertEquals(Set.of("nameB", "nameC", "vertex.~label"), new HashSet<>(sindexes));
        }
    }

    @Test
    public void testCardinality() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            final List<String> initialSindexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
            for (final String s : initialSindexes) {
                if (s.equals("vertex.~label")) {
                    g.call("aerospike.graph.admin.index.drop").
                            with("property_key", "~label").
                            with("element_type", "vertex").next();
                }
                g.call("aerospike.graph.admin.index.drop").
                        with("property_key", s).
                        with("element_type", "vertex").next();
            }
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameA").
                    with("element_type", "vertex").next();
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameB").
                    with("element_type", "vertex").next();
            Map<String, Long> nameAStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "nameA").
                    with("element_type", "vertex").next();
            Map<String, Long> nameBStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "nameB").
                    with("element_type", "vertex").next();
            while (nameAStatus.get("percent_complete") < 100 || nameBStatus.get("percent_complete") < 100) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                nameAStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "nameA").
                        with("element_type", "vertex").next();
                nameBStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "nameB").
                        with("element_type", "vertex").next();
            }
            final Map<String, Long> cardinality = (Map<String, Long>) g.call("aerospike.graph.admin.index.cardinality").next();
            Assert.assertTrue(cardinality.get("nameA") > 0);
        }
    }

    @Test
    public void testCreateDropRemoves() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            fireflyGraph.getBaseGraph().dropGraphIndices(fireflyGraph);
            Assert.assertTrue(((List<String>) g.call("aerospike.graph.admin.index.list").next()).isEmpty());
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameA").
                    with("element_type", "vertex").next();
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "~label").
                    with("element_type", "vertex").next();
            Map<String, Long> nameAStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "nameA").
                    with("element_type", "vertex").next();
            Map<String, Long> labelStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "~label").
                    with("element_type", "vertex").next();
            while (nameAStatus.get("percent_complete") < 100 ||
                    labelStatus.get("percent_complete") < 100) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                nameAStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "nameA").
                        with("element_type", "vertex").next();
                labelStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "~label").
                        with("element_type", "vertex").next();
            }
            Assert.assertEquals(Set.of("nameA", "vertex.~label"), new HashSet<>(((List<String>) g.call("aerospike.graph.admin.index.list").next())));
            g.call("aerospike.graph.admin.index.drop").
                    with("property_key", "nameA").
                    with("element_type", "vertex").next();
            g.call("aerospike.graph.admin.index.drop").
                    with("property_key", "~label").
                    with("element_type", "vertex").next();
            Assert.assertTrue(((List<String>) g.call("aerospike.graph.admin.index.list").next()).isEmpty());
        }
    }

    @Test
    public void testStatus() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            fireflyGraph.getBaseGraph().dropGraphIndices(fireflyGraph);
            Assert.assertTrue(((List<String>) g.call("aerospike.graph.admin.index.list").next()).isEmpty());
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameA").
                    with("element_type", "vertex").next();
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "~label").
                    with("element_type", "vertex").next();
            Map<String, Long> nameAStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "nameA").
                    with("element_type", "vertex").next();
            Map<String, Long> labelStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "~label").
                    with("element_type", "vertex").next();
            while (nameAStatus.get("percent_complete") < 100) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                nameAStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "nameA").
                        with("element_type", "vertex").next();
                labelStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "~label").
                        with("element_type", "vertex").next();
            }
            Assert.assertTrue(nameAStatus.get("load_time") > 0);
            Assert.assertEquals(100, (long) nameAStatus.get("percent_complete"));
        }
    }

    @Test
    public void testInvalid() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            try {
                g.call("aerospike.graph.admin.index.create").
                        with("property_key", "nameA").
                        with("element_type", "vertex").next();
                fail("Expected invalid parameter to fail");
            } catch (Exception ignored) {
            }
            try {
                g.call("aerospike.graph.admin.index.drop").
                        with("property_key", "vertex").
                        with("element_type1", "name").next();
                fail("Expected invalid parameter to fail");
            } catch (Exception ignored) {
            }
            try {
                g.call("aerospike.graph.admin.index.status").
                        with("property_key1", "vertex").
                        with("element_type", "name").next();
                fail("Expected invalid parameter to fail");
            } catch (Exception ignored) {
            }
            try {
                g.call("aerospike.graph.admin.index.status").
                        with("element_type", "name").next();
                fail("Expected invalid parameter to fail");
            } catch (Exception ignored) {
            }
            try {
                g.call("aerospike.graph.admin.index.cardinality").
                        with("element_type", "name").next();
                fail("Expected invalid parameter to fail");
            } catch (Exception ignored) {
            }
            try {
                g.call("aerospike.graph.admin.index.list").
                        with("element_type", "name").next();
                fail("Expected invalid parameter to fail");
            } catch (Exception ignored) {
            }
        }
    }
}
