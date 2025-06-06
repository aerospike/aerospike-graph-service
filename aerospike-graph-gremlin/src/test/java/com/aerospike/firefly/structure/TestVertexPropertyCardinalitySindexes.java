package com.aerospike.firefly.structure;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.aerospike.client.query.IndexType.STRING;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestVertexPropertyCardinalitySindexes {
    private static FireflyGraph graph = null;
    private static GraphTraversalSource g = null;

    @BeforeClass
    public static void setUp() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "name,age");
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }

    @AfterClass
    public static void tearDown() {
        if (graph != null) {
            graph.db.dropDatabase(graph, true);
            graph.close();
        }
    }

    @Before
    public void before() {
        //g.V().drop().iterate();
    }

    // Wait until the index is fully registered across the cluster
    private void waitForIndexToBeQueryable(AerospikeClient client, String namespace, String indexName) throws InterruptedException {
        int maxTries = 30;
        int delayMs = 500;

        for (int i = 0; i < maxTries; i++) {
            try {
                String response = Info.request(client.getNodes()[0], "sindex/" + namespace);
                if (response.contains(indexName)) {
                    return; // Index is now visible to queries
                }
            } catch (Exception ignored) {}

            Thread.sleep(delayMs);
        }
        throw new RuntimeException("Index " + indexName + " not registered in time.");
    }

    @Test
    public void testCliennt () throws InterruptedException {
        final String prefix = graph.db.getVpIndexPrefix();
        final String indexName = "name";
        final String binName = graph.db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN;
        final String formattedIndex = String.format("%s_%s", prefix, indexName);
        final Long indexSchema = graph.db.schemaManager.getVertexPropertyWrite(indexName);
        final String formattedIndexFinal = formattedIndex + "_" + STRING;
        //IndexTask task = graph.db.client.createIndex(null,
        //        graph.db.getNamespace(),
        //        graph.db.setFromElementType(FireflyVertex.class),
        //        formattedIndexFinal,
        //        binName,
        //        IndexType.STRING,
        //        IndexCollectionType.MAPVALUES,
        //        CTX.mapKey(Value.get(indexSchema)));
        //task.waitTillComplete();
        //waitForIndexToBeQueryable((AerospikeClient) graph.db.client, graph.db.getNamespace(), formattedIndexFinal);
        //for (Node node : graph.db.client.getNodes()) {
        //    String info = Info.request(node, "sindex-list");
        //    System.out.println("Node " + node.getName() + " index-list:\n" + info);
        //}
        final Key key = new Key("test", graph.db.VERTEX_AERO_SET, "test");
        final Map<Long, Object> innerMap = new TreeMap<>();
        innerMap.put(1L, "testValue1");
        innerMap.put(2L, "testValue1");
        innerMap.put(3L, 8);

        final Map<Long, Map<Long, Object>> outerMap = new TreeMap<>();
        outerMap.put(indexSchema, innerMap);

        final Bin bin = new Bin(binName, outerMap);
        graph.db.client.put(null, key, bin);

        com.aerospike.client.Record stored = graph.db.client.get(null, key);
        System.out.println("Stored record: " + stored.bins);

        Thread.sleep(1000);
       //final Filter filter = Filter.contains(binName, IndexCollectionType.MAPVALUES, "testValue1", CTX.mapKey(Value.get(indexSchema)));
       //final Statement statement = new Statement();
       //statement.setNamespace(graph.db.getNamespace());
       //statement.setSetName(graph.db.setFromElementType(FireflyVertex.class));
       //statement.setIndexName(formattedIndexFinal);
       //statement.setFilter(filter);
       //statement.setBinNames(binName);

        //final RecordSet rs = graph.db.client.query(null, statement);
        //while (rs.next()) {
        //    final Key key1 = rs.getKey();
        //    final Map<String, Object> bins = rs.getRecord().bins;
        //    System.out.println("Key: " + key1 + ", Bins: " + bins);
        //}
        System.out.println(g.V().has("name", P.gt(5)).toList());
    }

    @Test
    public void testVPSindex_MultipleWrittenWithVertex_StringType() {
        final Vertex actualVertex = g.addV("testVPSindex_MultipleWrittenWithVertex_StringType").
                property(VertexProperty.Cardinality.list, "name", "Simon").
                property(VertexProperty.Cardinality.list, "name", "Lyndon").next();

        final Vertex simonVertex = g.V().has("name", "Simon").next();
        final Vertex lyndonVertex = g.V().has("name", "Lyndon").next();
        final Vertex simonLyndonVertex1 = g.V().has("name", P.within("Simon", "Lyndon")).next();
        final Vertex simonLyndonVertex2 = g.V().has("name", P.within("Lyndon", "Simon")).next();
        final Vertex simonLyndonVertex3 = g.V().has("name", "Simon").has("name", "Lyndon").next();
        final TraversalMetrics simonVertexMetrics = g.V().has("name", "Simon").profile().next();
        final TraversalMetrics lyndonVertexMetrics = g.V().has("name", "Lyndon").profile().next();
        final TraversalMetrics simonLyndonVertex1Metrics = g.V().has("name", P.within("Simon", "Lyndon")).profile().next();
        final TraversalMetrics simonLyndonVertex2Metrics = g.V().has("name", P.within("Lyndon", "Simon")).profile().next();
        final TraversalMetrics simonLyndonVertex3Metrics = g.V().has("name", "Simon").has("name", "Lyndon").profile().next();

        final Metrics simonVertexMetricsFireflyMetric = (Metrics) simonVertexMetrics.getMetrics().toArray()[3];
        Assert.assertEquals(0, simonVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().size());
        final Metrics lyndonVertexMetricsFireflyMetric = (Metrics) lyndonVertexMetrics.getMetrics().toArray()[3];
        Assert.assertEquals(0, lyndonVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().size());
        final Metrics simonLyndonVertex1MetricsFireflyMetric = (Metrics) simonLyndonVertex1Metrics.getMetrics().toArray()[3];
        Assert.assertEquals(0, simonLyndonVertex1MetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().size());
        final Metrics simonLyndonVertex2MetricsFireflyMetric = (Metrics) simonLyndonVertex2Metrics.getMetrics().toArray()[3];
        Assert.assertEquals(0, simonLyndonVertex2MetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().size());
        final Metrics simonLyndonVertex3MetricsFireflyMetric = (Metrics) simonLyndonVertex3Metrics.getMetrics().toArray()[3];
        Assert.assertEquals(0, simonLyndonVertex3MetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().size());

        Assert.assertEquals(actualVertex, simonVertex);
        Assert.assertEquals(actualVertex, lyndonVertex);
        Assert.assertEquals(actualVertex, simonLyndonVertex1);
        Assert.assertEquals(actualVertex, simonLyndonVertex2);
        Assert.assertEquals(actualVertex, simonLyndonVertex3);
    }
}
