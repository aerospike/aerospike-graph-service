package com.aerospike.firefly.admin;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
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
            final URL url = new URL("http://localhost:9090/0/admin/index/list");
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
            final URL url = new URL("http://localhost:9090/0/admin/index/create?" + query);
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
            final URL url = new URL("http://localhost:9090/0/admin/index/drop?" + query);
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
            final URL url = new URL("http://localhost:9090/0/admin/index/cardinality");
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

    public String adminMetadataSummary() {
        try {
            final URL url = new URL("http://localhost:9090/0/admin/metadata/summary");
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

    public String adminMetadataUsage() {
        try {
            final URL url = new URL("http://localhost:9090/0/admin/metadata/usage");
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

    public String adminMetadataConfig() {
        try {
            final URL url = new URL("http://localhost:9093/0/admin/metadata/config");
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

    public String adminMetadataVersion() {
        try {
            final URL url = new URL("http://localhost:9094/0/admin/metadata/version");
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
            Assert.assertEquals("[ ]", indexList);


            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameB").
                    with("element_type", "vertex").next();
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "nameC").
                    with("element_type", "vertex").next();
            g.call("aerospike.graph.admin.index.create").
                    with("property_key", "~label").
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
            while (nameBStatus.get("percent_complete") < 100 || nameCStatus.get("percent_complete") < 100 ||
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
            final String indexListAfterCreate = adminIndexList();
            final Set<String> indexes = convertStringListToSet(indexListAfterCreate);
            Assert.assertEquals(Set.of("\"nameB\"", "\"nameC\"", "\"vertex.~label\""), indexes);
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
            g.V().drop().iterate();
            g.addV("person").property("nameA", "Alice").property("nameB", "Bob").next();
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
                    Thread.sleep(1);
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

            // Give time for index cardinality to be updated.
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            final String cardinality = adminIndexCardinality();
            final String[] cardinalityArray = cardinality.substring(1, cardinality.length() - 1).split(",");
            final Set<String> cardinalitySet = new HashSet<>();
            for (final String s : cardinalityArray) {
                cardinalitySet.add(s.trim());
            }
            Assert.assertEquals(Set.of("\"nameA\" : 1", "\"nameB\" : 1"), cardinalitySet);
        }
    }

    @Test
    public void testCardinalityInteger() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.V().drop().iterate();
            g.addV("person").property("nameA", 1).property("nameB", 2).next();
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
                    Thread.sleep(1);
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
            Assert.assertEquals(Set.of("\"nameA\" : 1", "\"nameB\" : 1"), cardinalitySet);
        }
    }

    @Test
    public void testSummary() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            final String summary = adminMetadataSummary();
            Assert.assertTrue(summary.contains("Total vertex count"));
        }
    }

    @Test
    public void testUsage() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            final String usage = adminMetadataUsage();
            Assert.assertTrue(usage.contains("raw"));
        }
    }

    @Test
    public void testConfig() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.client.password", "Foo");
        config.setProperty("aerospike.graph.http.port", 9093);

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final String configString = adminMetadataConfig();
            Assert.assertTrue(configString.startsWith("{"));
            Assert.assertTrue(configString.endsWith("}"));
            final ObjectReader reader = new ObjectMapper().reader();
            final JsonNode tree = reader.readTree(configString);
            // Check has Aerospike version and Aerospike Graph Service version
            Assert.assertTrue(tree.has("Gremlin Server Configuration"));
            Assert.assertTrue(tree.has("Graph Properties"));
            // Check value of graph service version
            final JsonNode configNode = tree.get("Graph Properties");
            Assert.assertTrue(configNode.has("aerospike.client.password"));
            Assert.assertTrue(configNode.has("aerospike.graph.data.model"));
            final String pw = configNode.get("aerospike.client.password").asText();
            Assert.assertEquals(pw, "********");
            final String dm = configNode.get("aerospike.graph.data.model").asText();
            Assert.assertEquals(dm, "packed");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testVersion() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.http.port", 9094);

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final String version = adminMetadataVersion();
            Assert.assertTrue(version.startsWith("{"));
            Assert.assertTrue(version.endsWith("}"));
            final ObjectReader reader = new ObjectMapper().reader();
            final JsonNode tree = reader.readTree(version);
            // Check has Aerospike version and Aerospike Graph Service version
            Assert.assertTrue(tree.has("Aerospike version"));
            Assert.assertTrue(tree.has("Aerospike Graph Service version"));
            // Check value of graph service version
            final String graphServiceVersion = tree.get("Aerospike Graph Service version").asText();
            Assert.assertEquals(FireflyGraph.FIREFLY_VERSION, graphServiceVersion);
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
