package com.aerospike.firefly.admin;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestAdminCallHttp {

    public String adminIndexList() {
        try {
            final URL url = new URL("http://localhost:9090/admin/index/list");
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();
            // Send request to the server and read reply
            con.setRequestMethod("GET");

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String adminIndexCreate(final String propertyKey, final String elementType) {
        try {
            final String query = String.format("property_key=%s&element_type=%s",
                    URLEncoder.encode(propertyKey, "UTF-8"),
                    URLEncoder.encode(elementType, "UTF-8"));
            final URL url = new URL("http://localhost:9090/admin/index/create?" + query);
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String adminIndexDrop(final String propertyKey, final String elementType) {
        try {
            final String query = String.format("property_key=%s&element_type=%s",
                    URLEncoder.encode(propertyKey, "UTF-8"),
                    URLEncoder.encode(elementType, "UTF-8"));
            final URL url = new URL("http://localhost:9090/admin/index/drop?" + query);
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String adminIndexCardinality() {
        try {
            final URL url = new URL("http://localhost:9090/admin/index/cardinality");
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            throw new RuntimeException(e);
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
            final String indexList = adminIndexList();
            Assert.assertEquals("[]", indexList);


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
            while (nameBStatus.get("percent_complete") < 100 || nameCStatus.get("percent_complete") < 100) {
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
            }
            final String indexListAfterCreate = adminIndexList();
            final Set<String> indexes = convertStringListToSet(indexListAfterCreate);
            Assert.assertEquals(Set.of("nameB", "nameC"), indexes);
        }
    }

    public static Set<String> convertStringListToSet(final String listString) {
        final String[] indexesArray = listString.substring(1, listString.length() - 1).split(",");
        final Set<String> indexes = new HashSet<>();
        for (String index : indexesArray) {
            indexes.add(index.trim());
        }
        return indexes;
    }

    @Test
    public void testCreate() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            fireflyGraph.getBaseGraph().dropGraphIndices(fireflyGraph);
            Assert.assertTrue(((List<String>) g.call("aerospike.graph.admin.index.list").next()).isEmpty());
            adminIndexCreate("nameA", "vertex");
            Map<String, Long> nameAStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "nameA").
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
            }
            Assert.assertEquals(100L, nameAStatus.get("percent_complete").longValue());
        }
    }

    @Test
    public void testDrop() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            fireflyGraph.getBaseGraph().dropGraphIndices(fireflyGraph);
            final List<String> indexesAfterDrop = (List<String>) g.call("aerospike.graph.admin.index.list").next();
            Assert.assertTrue(indexesAfterDrop.isEmpty());
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameB").
                    with("element_type", "vertex").next();
            Map<String, Long> nameBStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                    with("property_key", "nameB").
                    with("element_type", "vertex").next();
            while (nameBStatus.get("percent_complete") < 100) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                nameBStatus = (Map<String, Long>) g.call("aerospike.graph.admin.index.status").
                        with("property_key", "nameB").
                        with("element_type", "vertex").next();
            }
            adminIndexDrop("nameB", "vertex");
            final List<String> indexesAfterDrop2 = (List<String>) g.call("aerospike.graph.admin.index.list").next();
            Assert.assertTrue(indexesAfterDrop2.isEmpty());
        }
    }

    @Test
    public void testCardinality() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.addV("person").property("nameA", "Alice").property("nameB", "Bob2").next();
            g.addV("person").property("nameA", "Alice").property("nameB", "Bob").next();
            final List<String> initialSindexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
            for (final String s : initialSindexes) {
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
            final String cardinality = adminIndexCardinality();
            final String[] cardinalityArray = cardinality.substring(1, cardinality.length() - 1).split(",");
            final Set<String> cardinalitySet = new HashSet<>();
            for (final String s : cardinalityArray) {
                cardinalitySet.add(s.trim());
            }
            Assert.assertEquals(Set.of("nameA=1", "nameB=2"), cardinalitySet);
        }
    }
}
