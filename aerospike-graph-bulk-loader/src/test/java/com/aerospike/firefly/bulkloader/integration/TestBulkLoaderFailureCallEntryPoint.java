package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.bulkloader.integration.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.process.call.bulkload.FireflyBulkLoaderServiceFactory.BULK_LOAD_SUCCESS;
import static com.aerospike.firefly.process.call.bulkload.FireflyBulkLoaderServiceFactory.formatErrorCount;

public class TestBulkLoaderFailureCallEntryPoint {
    @Before
    public void beforeEach() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
        }
    }

    @Test
    public void allowDuplicateVertexId() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final String output = (String) fireflyGraph.traversal().call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/duplicate-vertex-id.properties").next();
            Assert.assertNotEquals(BULK_LOAD_SUCCESS, output);
            Assert.assertEquals(formatErrorCount(fireflyGraph), output);
            final Map<String, Long> errorCounts = (Map<String, Long>) fireflyGraph.traversal().call("get-bulk-load-error-count").next();
            Assert.assertEquals(3, (long) errorCounts.get("duplicate-vertex-id-count"));
            Assert.assertEquals(0, (long) errorCounts.get("bad-edge-count"));
            Assert.assertEquals(0, (long) errorCounts.get("bad-entry-count"));

            final GraphTraversal duplicateVertexInfos =  fireflyGraph.traversal().call("get-bulk-load-errors").with("type", "duplicate-vertex-ids");
            long duplicateCount = 0;
            final Set<Object> duplicatedIds = new HashSet<>();
            duplicatedIds.add(5l);
            duplicatedIds.add(7l);
            while (duplicateVertexInfos.hasNext()) {
                final Map<String, Object> duplicateVertexInfo = (Map<String, Object>) duplicateVertexInfos.next();
                duplicateCount += (long) duplicateVertexInfo.get("count") - 1;
                duplicatedIds.remove(duplicateVertexInfo.get("id"));
            }
            Assert.assertEquals(3, duplicateCount);
            Assert.assertTrue(duplicatedIds.isEmpty());

            final GraphTraversal badEdgeInfos =  fireflyGraph.traversal().call("get-bulk-load-errors").with("type", "bad-edges");
            Assert.assertFalse(badEdgeInfos.hasNext());
            final GraphTraversal badEntryInfos =  fireflyGraph.traversal().call("get-bulk-load-errors").with("type", "bad-entries");
            Assert.assertFalse(badEntryInfos.hasNext());
        }
    }

    @Test
    public void allowBadEdges() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final String output = (String) fireflyGraph.traversal().call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/detached-edges.properties").next();
            Assert.assertNotEquals(BULK_LOAD_SUCCESS, output);
            Assert.assertEquals(formatErrorCount(fireflyGraph), output);
            final Map<String, Long> errorCounts = (Map<String, Long>) fireflyGraph.traversal().call("get-bulk-load-error-count").next();
            Assert.assertEquals(0, (long) errorCounts.get("duplicate-vertex-id-count"));
            Assert.assertEquals(5, (long) errorCounts.get("bad-edge-count"));
            Assert.assertEquals(0, (long) errorCounts.get("bad-entry-count"));

            final GraphTraversal badEdgeInfos =  fireflyGraph.traversal().call("get-bulk-load-errors").with("type", "bad-edges");
            long badEdgeCount = 0;
            final Map<Object, Long> badEdges = new HashMap();
            badEdges.put("Roma", 2L);
            badEdges.put("kyle", 3L);

            while (badEdgeInfos.hasNext()) {
                final Map<String, Object> badEdgeInfo = (Map<String, Object>) badEdgeInfos.next();
                badEdgeCount += (long) badEdgeInfo.get("count");
                Assert.assertEquals(badEdges.get(badEdgeInfo.get("bad-vertex-id")), badEdgeInfo.get("count"));
                badEdges.remove(badEdgeInfo.get("bad-vertex-id"));
            }
            Assert.assertEquals(5, badEdgeCount);
            Assert.assertTrue(badEdges.isEmpty());

            final GraphTraversal duplicateVertexInfos =  fireflyGraph.traversal().call("get-bulk-load-errors").with("type", "duplicate-vertex-ids");
            Assert.assertFalse(duplicateVertexInfos.hasNext());
            final GraphTraversal badEntryInfos =  fireflyGraph.traversal().call("get-bulk-load-errors").with("type", "bad-entries");
            Assert.assertFalse(badEntryInfos.hasNext());
        }
    }

    @Test
    public void allowBadEntries() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final String output = (String) fireflyGraph.traversal().call("bulk-load").with("aerospike.graphloader.config", "src/test/resources/conf/packed/bad-entries.properties").next();
            Assert.assertNotEquals(BULK_LOAD_SUCCESS, output);
            Assert.assertEquals(formatErrorCount(fireflyGraph), output);
            final Map<String, Long> errorCounts = (Map<String, Long>) fireflyGraph.traversal().call("get-bulk-load-error-count").next();
            Assert.assertEquals(0, (long) errorCounts.get("duplicate-vertex-id-count"));
            Assert.assertEquals(0, (long) errorCounts.get("bad-edge-count"));
            Assert.assertEquals(3, (long) errorCounts.get("bad-entry-count"));

            final GraphTraversal badEntryInfos =  fireflyGraph.traversal().call("get-bulk-load-errors").with("type", "bad-entries");
            int badEntryCount = 0;
            final Set<String> badFiles = new HashSet<>();
            badFiles.add("models.csv");
            badFiles.add("drives.csv");
            badFiles.add("people.csv");
            final Set<String> badRows = new HashSet<>();
            badRows.add("[1000,model,null,null,null,null,Tesla,Model 3,twentynineteen,null,null,null,null,null,null,null]");
            badRows.add("[20,drives,null,GR86,null,null,null,null,null,null,null]");
            badRows.add("[imposter,person,Marko,notAnInt,Aerospike,false,null,null,null,null,null,null,null,null,null,null]");

            while (badEntryInfos.hasNext()) {
                final Map<String, String> badEntryInfo = (Map<String, String>) badEntryInfos.next();
                badEntryCount++;
                final String file = badEntryInfo.get("file");
                badFiles.remove(file.substring(file.lastIndexOf('/') + 1));
                badRows.remove(badEntryInfo.get("row"));
            }
            Assert.assertEquals(3, badEntryCount);
            Assert.assertTrue(badFiles.isEmpty());
            Assert.assertTrue(badRows.isEmpty());

            final GraphTraversal duplicateVertexInfos =  fireflyGraph.traversal().call("get-bulk-load-errors").with("type", "duplicate-vertex-ids");
            Assert.assertFalse(duplicateVertexInfos.hasNext());
            final GraphTraversal badEdgeInfos =  fireflyGraph.traversal().call("get-bulk-load-errors").with("type", "bad-edges");
            Assert.assertFalse(badEdgeInfos.hasNext());
        }
    }

    @Test
    public void testGetErrorsInvalidParam() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            try {
                g.call("get-bulk-load-errors").next();
                Assert.fail("get-bulk-load-errors succeeded when failure expected due to no params");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("\"get-bulk-load-errors\" must be invoked with parameter \"type\" and only one of the following values:"));
            }
            try {
                g.call("get-bulk-load-errors").with("type").next();
                Assert.fail("get-bulk-load-errors succeeded when failure expected due to param with key and no value");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("\"get-bulk-load-errors\" must be invoked with parameter \"type\" and only one of the following values:"));
            }
            try {
                g.call("get-bulk-load-errors").with("type", "foo").next();
                Assert.fail("get-bulk-load-errors succeeded when failure expected due to param with bad value");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("\"get-bulk-load-errors\" must be invoked with parameter \"type\" and only one of the following values:"));
            }
            try {
                g.call("get-bulk-load-errors").with("foo", "bad-edges").next();
                Assert.fail("get-bulk-load-errors succeeded when failure expected due to param with bad key");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().startsWith("\"get-bulk-load-errors\" must be invoked with parameter \"type\" and only one of the following values:"));
            }
        }
    }
}
