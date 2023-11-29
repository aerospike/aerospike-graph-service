package com.aerospike.firefly;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.schema.EdgeSchema;
import com.aerospike.firefly.schema.GraphSchema;
import com.aerospike.firefly.schema.PropertySchema;
import com.aerospike.firefly.schema.VertexSchema;
import com.aerospike.firefly.sizing.SizingTool;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestSizingToolAccuracy {
    private static final Map<String, Path> INTEGRATION_TEST_CONFIGURATIONS = ImmutableMap.of(
            FireflyGraph.DATA_MODEL, Path.of("../conf/integration-test-settings-packed.properties"),
            FireflyGraph.DATA_MODEL + "-sindex", Path.of("../conf/integration-test-settings-packed-sindex.properties")
    );

    public static final Path INTEGRATION_TEST_PROPERTIES;
    private static final Boolean DEBUG_BIN_SIZES = false;


    static {
        // Default to packed, as it is currently our 'suggested' data model.
        final String integrationTestProperties = System.getProperty("integration.test.properties");
        final Path packed = Path.of("../conf/integration-test-settings-packed.properties");
        if (System.getProperty("integration.test.properties") != null) {
            INTEGRATION_TEST_PROPERTIES = INTEGRATION_TEST_CONFIGURATIONS.getOrDefault(integrationTestProperties, packed);
        } else {
            INTEGRATION_TEST_PROPERTIES = packed;
        }
    }

    private List<PropertySchema> createPropertySchemas(final List<String> key, final List<String> type, final List<Number> size, final List<Number> likelihood, final List<Boolean> sindexed) {
        final List<PropertySchema> propertySchemas = new ArrayList<>();
        for (int i = 0; i < key.size(); i++) {
            final PropertySchema propertySchema = new PropertySchema();
            propertySchema.key = key.get(i);
            propertySchema.type = type.get(i);
            propertySchema.size = size.get(i);
            propertySchema.likelihood = likelihood.get(i);
            propertySchema.sindexed = sindexed.get(i);
            propertySchemas.add(propertySchema);
        }
        return propertySchemas;
    }

    private VertexSchema createVertexSchema(final String label, final int count, final List<PropertySchema> propertySchemas) {
        final VertexSchema vertexSchema = new VertexSchema();
        vertexSchema.label = label;
        vertexSchema.count = count;
        vertexSchema.properties = propertySchemas;
        return vertexSchema;
    }

    private EdgeSchema createEdgeSchema(final String label, final int count, final List<PropertySchema> propertySchemas) {
        final EdgeSchema edgeSchema = new EdgeSchema();
        edgeSchema.label = label;
        edgeSchema.count = count;
        edgeSchema.properties = propertySchemas;
        return edgeSchema;
    }

    @Test
    public void testPeopleWithDogSchemaAccuracy() throws Exception {
        final GraphSchema schema = new GraphSchema();
        schema.vertexSchema = new ArrayList<>();
        schema.edgeSchema = new ArrayList<>();
        schema.vertexSchema.add(createVertexSchema("person", 1000, createPropertySchemas(
                List.of("name", "age", "location"), List.of("string", "integer", "string"), List.of(10, 4, 10), List.of(1.0, 1.0, 1.0), List.of(true, false, false)
        )));
        schema.vertexSchema.add(createVertexSchema("dog", 500, createPropertySchemas(
                List.of("name", "age"), List.of("string", "integer"), List.of(10, 4), List.of(1.0, 1.0), List.of(true, false)
        )));
        schema.edgeSchema.add(createEdgeSchema("knows", 10000, createPropertySchemas(
                List.of("since"), List.of("integer"), List.of(4), List.of(1.0), List.of(false)
        )));
        schema.edgeSchema.add(createEdgeSchema("owns", 500, createPropertySchemas(
                List.of("since"), List.of("integer"), List.of(4), List.of(1.0), List.of(false)
        )));
        compareSchemaToActual(schema, 0.15f);
    }

    @Test
    public void testHeavilyConnectedGraph() {
        final GraphSchema schema = new GraphSchema();
        schema.vertexSchema = new ArrayList<>();
        schema.edgeSchema = new ArrayList<>();
        schema.vertexSchema.add(createVertexSchema("person", 10, createPropertySchemas(
                List.of("name", "age", "location"), List.of("string", "integer", "string"), List.of(10, 4, 10), List.of(1.0, 1.0, 1.0), List.of(true, false, false)
        )));
        schema.edgeSchema.add(createEdgeSchema("knows", 10000, createPropertySchemas(
                List.of("since"), List.of("integer"), List.of(4), List.of(1.0), List.of(false)
        )));
        compareSchemaToActual(schema, 0.15f);
    }

    @Test
    public void testHardlyConnectedGraph() {
        final GraphSchema schema = new GraphSchema();
        schema.vertexSchema = new ArrayList<>();
        schema.edgeSchema = new ArrayList<>();
        schema.vertexSchema.add(createVertexSchema("person", 10000, createPropertySchemas(
                List.of("name", "age", "location"), List.of("string", "integer", "string"), List.of(10, 4, 10), List.of(1.0, 1.0, 1.0), List.of(true, false, false)
        )));
        schema.edgeSchema.add(createEdgeSchema("knows", 1000, createPropertySchemas(
                List.of("since"), List.of("integer"), List.of(4), List.of(1.0), List.of(false)
        )));
        compareSchemaToActual(schema, 0.15f);
    }

    @Test
    public void testAddEdgesGraph() {
        final GraphSchema schema = new GraphSchema();
        schema.vertexSchema = new ArrayList<>();
        schema.edgeSchema = new ArrayList<>();
        schema.vertexSchema.add(createVertexSchema("person", 1, createPropertySchemas(
                List.of(), List.of(), List.of(), List.of(), List.of()
        )));
        schema.edgeSchema.add(createEdgeSchema("knows", 5000, createPropertySchemas(
                List.of(), List.of(), List.of(), List.of(), List.of()
        )));
        compareSchemaToActual(schema, 0.15f);
    }

    @Test
    public void testUnconnectedGraph() {
        final GraphSchema schema = new GraphSchema();
        schema.vertexSchema = new ArrayList<>();
        schema.edgeSchema = new ArrayList<>();
        schema.vertexSchema.add(createVertexSchema("person", 1, createPropertySchemas(
                List.of("name", "age", "location"), List.of("string", "integer", "string"), List.of(10, 8, 10), List.of(1.0, 1.0, 1.0), List.of(true, false, false)
        )));
        compareSchemaToActual(schema, 0.15f);
    }

    @Test
    public void testSingleVertexStringPropertiesGraph() {
        final GraphSchema schema = new GraphSchema();
        schema.vertexSchema = new ArrayList<>();
        schema.edgeSchema = new ArrayList<>();
        schema.vertexSchema.add(createVertexSchema("person", 1, createPropertySchemas(
                List.of("s1", "s2", "s3", "s4", "s5", "s6"), List.of("string", "string", "string", "string", "string", "string"), List.of(10, 10, 10, 10, 10, 10), List.of(1.0, 1.0, 1.0, 1.0, 1.0, 1.0), List.of(true, false, false, false, false, false)
        )));
        compareSchemaToActual(schema, 0.15f);
    }

    @Test
    public void testSingleVertexIntegerGraph() {
        final GraphSchema schema = new GraphSchema();
        schema.vertexSchema = new ArrayList<>();
        schema.edgeSchema = new ArrayList<>();
        schema.vertexSchema.add(createVertexSchema("person", 1, createPropertySchemas(
                List.of("s1", "s2", "s3", "s4", "s5", "s6"), List.of("integer", "integer", "integer", "integer", "integer", "integer"), List.of(10, 10, 10, 10, 10, 10), List.of(1.0, 1.0, 1.0, 1.0, 1.0, 1.0), List.of(true, false, false, false, false, false)
        )));
        compareSchemaToActual(schema, 0.15f);
    }

    @Test
    public void testSingleVertexLongKey() {
        final GraphSchema schema = new GraphSchema();
        schema.vertexSchema = new ArrayList<>();
        schema.edgeSchema = new ArrayList<>();
        schema.vertexSchema.add(createVertexSchema("person", 1, createPropertySchemas(
                List.of("thisisaverylongkeyaimedatstrictlytestinghowwellpropertykeysareestimated" +
                        "thisisaverylongkeyaimedatstrictlytestinghowwellpropertykeysareestimated" +
                        "thisisaverylongkeyaimedatstrictlytestinghowwellpropertykeysareestimated" +
                        "thisisaverylongkeyaimedatstrictlytestinghowwellpropertykeysareestimated" +
                        "thisisaverylongkeyaimedatstrictlytestinghowwellpropertykeysareestimated" +
                        "thisisaverylongkeyaimedatstrictlytestinghowwellpropertykeysareestimated"), List.of("string"), List.of(1), List.of(1.0), List.of(false)
        )));
        compareSchemaToActual(schema, 0.15f);
    }

    @Test
    public void testSingleVertexLongValue() {
        final GraphSchema schema = new GraphSchema();
        schema.vertexSchema = new ArrayList<>();
        schema.edgeSchema = new ArrayList<>();
        schema.vertexSchema.add(createVertexSchema("person", 1, createPropertySchemas(
                List.of("s"), List.of("string"), List.of(500), List.of(1.0), List.of(false)
        )));
        compareSchemaToActual(schema, 0.15f);
    }

    void compareSchemaToActual(final GraphSchema schema, final float tolerance) {
        generateGraph(schema);

        final SizingTool sizingTool = new SizingTool(schema);
        long vertexRecordCountEstimate = sizingTool.estimateVertexRecordCount();
        long vertexRecordTotalSizeEstimate = sizingTool.estimateAverageVertexRecordSize() * vertexRecordCountEstimate;
        long edgeRecordCountEstimate = sizingTool.estimateEdgeRecordCount();
        long edgeRecordTotalSizeEstimate = sizingTool.estimateAverageEdgeRecordSize() * edgeRecordCountEstimate;

        long actualVertexRecordCount = getVertexRecordCount();
        long actualVertexRecordTotalSize = getVertexRecordTotalSize();
        long actualEdgeRecordCount = getEdgeRecordCount();
        long actualEdgeRecordTotalSize = getEdgeRecordTotalSize();

        if (Math.abs(vertexRecordCountEstimate - actualVertexRecordCount) > (vertexRecordCountEstimate * tolerance)) {
            Assert.fail("Vertex Record Count Estimate: " + vertexRecordCountEstimate + " Actual Vertex Record Count: " + actualVertexRecordCount + " Tolerance: " + tolerance);
        }
        if (Math.abs(vertexRecordTotalSizeEstimate - actualVertexRecordTotalSize) > (vertexRecordTotalSizeEstimate * tolerance)) {
            Assert.fail("Vertex Record Total Size Estimate: " + vertexRecordTotalSizeEstimate + " Actual Vertex Record Total Size: " + actualVertexRecordTotalSize + " Tolerance: " + tolerance);
        }
        if (Math.abs(edgeRecordCountEstimate - actualEdgeRecordCount) > (edgeRecordCountEstimate * tolerance)) {
            Assert.fail("Edge Record Count Estimate: " + edgeRecordCountEstimate + " Actual Edge Record Count: " + actualEdgeRecordCount + " Tolerance: " + tolerance);
        }
        if (Math.abs(edgeRecordTotalSizeEstimate - actualEdgeRecordTotalSize) > (edgeRecordTotalSizeEstimate * tolerance)) {
            Assert.fail("Edge Record Total Size Estimate: " + edgeRecordTotalSizeEstimate + " Actual Edge Record Total Size: " + actualEdgeRecordTotalSize + " Tolerance: " + tolerance);
        }
        System.out.println("Vertex Record Total Size Estimate: " + vertexRecordTotalSizeEstimate);
        System.out.println("Edge Record Total Size Estimate: " + edgeRecordTotalSizeEstimate);
        System.out.println("Actual Vertex Record Total Size: " + actualVertexRecordTotalSize);
        System.out.println("Actual Edge Record Total Size: " + actualEdgeRecordTotalSize);
    }

    public long getVertexRecordCount() {
        try (final AerospikeConnection connection = AerospikeConnection.connect(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            final AtomicLong count = new AtomicLong(0);
            final AerospikeClient client = connection.getClient();
            client.scanAll(null, connection.getNamespace(), connection.VERTEX_AERO_SET, (key, record) -> count.addAndGet(1));
            return count.get();
        }
    }

    public long getVertexRecordTotalSize() {
        try (final AerospikeConnection connection = AerospikeConnection.connect(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            final AtomicLong totalSize = new AtomicLong(0);
            final AerospikeClient client = connection.getClient();
            client.scanAll(null, connection.getNamespace(), connection.VERTEX_AERO_SET, (key, record) -> {
                record.bins.keySet().forEach(k -> {
                    if (DEBUG_BIN_SIZES) {
                        System.out.println("v=Key: " + k + " Value: " + Value.get(record.bins.get(k)).estimateSize());
                    }
                    totalSize.addAndGet(k.length());
                    totalSize.addAndGet(Value.get(record.bins.get(k)).estimateSize());
                });
            });
            return totalSize.get();
        }
    }

    public long getEdgeRecordCount() {
        try (final AerospikeConnection connection = AerospikeConnection.connect(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            final AtomicLong count = new AtomicLong(0);
            final AerospikeClient client = connection.getClient();
            client.scanAll(null, connection.getNamespace(), connection.EDGE_AERO_SET, (key, record) -> count.addAndGet(1));
            return count.get();
        }
    }

    public long getEdgeRecordTotalSize() {
        try (final AerospikeConnection connection = AerospikeConnection.connect(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            final AtomicLong totalSize = new AtomicLong(0);
            final AerospikeClient client = connection.getClient();
            client.scanAll(null, connection.getNamespace(), connection.EDGE_AERO_SET, (key, record) -> {
                record.bins.keySet().forEach(k -> {
                    if (DEBUG_BIN_SIZES) {
                        System.out.println("e=Key: " + k + " Value: " + Value.get(record.bins.get(k)).estimateSize());
                    }
                    totalSize.addAndGet(k.length());
                    totalSize.addAndGet(Value.get(record.bins.get(k)).estimateSize());
                });
            });
            return totalSize.get();
        }
    }

    public void generateGraph(final GraphSchema schema) {
        try (FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            g.V().drop().iterate();
            for (final VertexSchema vertexSchema : schema.vertexSchema) {
                for (int i = 0; i < vertexSchema.count.intValue(); i++) {
                    GraphTraversal<?, ?> addVertexTraversal = g.addV(vertexSchema.label);
                    for (final PropertySchema propertySchema : vertexSchema.properties) {
                        switch (propertySchema.type) {
                            case "string":
                                final StringBuilder stringBuilder = new StringBuilder();
                                final Random random = new Random();
                                final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
                                while (stringBuilder.length() < propertySchema.size.intValue()) {
                                    stringBuilder.append((int) (random.nextFloat() * CHARACTERS.length()));
                                }
                                addVertexTraversal = addVertexTraversal.property(propertySchema.key, stringBuilder.toString());
                                break;
                            case "integer":
                                addVertexTraversal = addVertexTraversal.property(propertySchema.key, Integer.MAX_VALUE);
                                break;
                            case "long":
                                addVertexTraversal = addVertexTraversal.property(propertySchema.key, Long.MAX_VALUE);
                                break;
                            case "float":
                                addVertexTraversal = addVertexTraversal.property(propertySchema.key, Float.MAX_VALUE);
                                break;
                            case "double":
                                addVertexTraversal = addVertexTraversal.property(propertySchema.key, Double.MAX_VALUE);
                                break;
                            case "boolean":
                                addVertexTraversal = addVertexTraversal.property(propertySchema.key, true);
                                break;
                        }
                    }
                    addVertexTraversal.iterate();
                }
            }

            List<Object> ids = g.V().id().toList();

            for (final EdgeSchema edgeSchema : schema.edgeSchema) {
                for (int i = 0; i < edgeSchema.count.intValue(); i++) {
                    GraphTraversal<?, ?> addEdge = g.addE(edgeSchema.label).
                            from(__.V(ids.get(new Random().nextInt(ids.size())))).
                            to(__.V(ids.get(new Random().nextInt(ids.size()))));
                    for (final PropertySchema propertySchema : edgeSchema.properties) {
                        switch (propertySchema.type) {
                            case "string":
                                final StringBuilder stringBuilder = new StringBuilder();
                                final Random random = new Random();
                                final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
                                while (stringBuilder.length() < propertySchema.size.intValue()) {
                                    stringBuilder.append((int) (random.nextFloat() * CHARACTERS.length()));
                                }
                                addEdge = addEdge.property(propertySchema.key, stringBuilder.toString());
                                break;
                            case "integer":
                            case "long":
                                addEdge = addEdge.property(propertySchema.key, 1);
                                break;
                            case "float":
                            case "double":
                                addEdge = addEdge.property(propertySchema.key, 1.0);
                                break;
                            case "boolean":
                                addEdge = addEdge.property(propertySchema.key, true);
                                break;
                        }
                    }

                    addEdge.iterate();
                }
            }
        }
    }
}
