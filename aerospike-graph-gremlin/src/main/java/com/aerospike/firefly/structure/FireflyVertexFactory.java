package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.LazyEdgeCacheIdTransform;
import com.aerospike.firefly.structure.id.LazyIdTransform;
import com.aerospike.firefly.structure.id.LazyVertexPropertyIdTransform;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FireflyVertexFactory {
    public static FireflyVertex create(final FireflyId fid,
                                       final String label,
                                       final FireflyGraph graph,
                                       final Map<String, List<LazyIdTransform>> inEdgeIds,
                                       final Map<String, List<LazyIdTransform>> outEdgeIds,
                                       final Map<String, LazyIdTransform> vertexPropertyIds,
                                       final Map<String, Object> vertexPropertyValues,
                                       final Map<String, Object> vertexPropertyValuesTypeHints,
                                       final Map<Object, Map<String, Object>> vertexPropertyProperties,
                                       final Map<Object, Map<String, Object>> vertexPropertyPropertiesTypeHints,
                                       final boolean isEdgeCacheOverflowed,
                                       final AerospikeConnection db) {

        return new FireflyVertex(
                fid,
                label,
                graph,
                inEdgeIds,
                outEdgeIds,
                vertexPropertyIds,
                vertexPropertyValues,
                vertexPropertyValuesTypeHints,
                vertexPropertyProperties,
                vertexPropertyPropertiesTypeHints,
                isEdgeCacheOverflowed,
                db);
    }

    /**
     * Construct vertex from KeyRecord.
     *
     * @param keyRecord KeyRecord to construct vertex with.
     * @param graph     FireflyGraph
     * @return FireflyVertex.
     */
    public static FireflyVertex create(final KeyRecord keyRecord, final FireflyGraph graph) {
        if (keyRecord == null) {
            return null;
        }

        final Record record = keyRecord.record;
        final AerospikeConnection db = graph.getBaseGraph();

        // Read the vertex's firefly record from the database
        if (record == null) {
            return null;
        }

        // Get id and label for vertex.
        final FireflyId id = graph.getIdFactory().createVertexIdFromRecord(keyRecord);
        final String label = graph.getBaseGraph().schemaManager.getVertexLabelString(record.getLong(db.LABEL_BIN));

        // Get cache state.
        final boolean edgeCacheOverflowed = record.getBoolean(db.EDGE_CACHE_DISABLED_BIN);

        // Get inEdgeIds and outEdgeIds.
        final Map<String, List<Object>> inEdgeIds = (Map) record.getMap(db.IN_EDGES_BIN);
        graph.getIdFactory().convertMapToLazyIdsInPlace(inEdgeIds, graph, LazyEdgeCacheIdTransform.class);
        final Map<String, List<Object>> outEdgeIds = (Map) record.getMap(db.OUT_EDGES_BIN);
        graph.getIdFactory().convertMapToLazyIdsInPlace(outEdgeIds, graph, LazyEdgeCacheIdTransform.class);
        final Map<String, List<LazyIdTransform>> fireflyInEdgeIds = (Map) inEdgeIds;
        final Map<String, List<LazyIdTransform>> fireflyOutEdgeIds = (Map) outEdgeIds;
        final Map<Object, Map<Long, Object>> vertexPropertyPropertiesDisk = (Map) record.getMap(db.PROPERTIES_BIN);
        final Map<Object, Map<String, Object>> vertexPropertyProperties = new TreeMap<>();
        db.schemaManager.populateVertexVpPropertySchemaMapToStringMap(vertexPropertyPropertiesDisk, vertexPropertyProperties);
        final Map<Object, Map<Long, Object>> vertexPropertyPropertiesTypeHintsDisk = (Map) record.getMap(db.TYPE_HINTS_BIN);
        final Map<Object, Map<String, Object>> vertexPropertyPropertiesTypeHints = new TreeMap<>();
        db.schemaManager.populateVertexVpPropertySchemaMapToStringMap(vertexPropertyPropertiesTypeHintsDisk, vertexPropertyPropertiesTypeHints);

        // Create vertex based on type hint.
        // Get vertex properties and vertex property counter from record.
        final Map<Long, Object> vertexPropertyValuesDisk =
                (Map<Long, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN);
        final Map<String, Object> vertexPropertyValues = new TreeMap<>();
        db.schemaManager.populateVertexPropertySchemaMapToStringMap(vertexPropertyValuesDisk, vertexPropertyValues);
        final Map<Long, Object> vertexPropertyTypeHintsDisk =
                (Map<Long, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN);
        final Map<String, Object> vertexPropertyTypeHints = new TreeMap<>();
        db.schemaManager.populateVertexPropertySchemaMapToStringMap(vertexPropertyTypeHintsDisk, vertexPropertyTypeHints);
        final Map<Long, Object> vertexPropertyIdsDisk =
                (Map<Long, Object>) record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID_BIN);
        final Map<String, Object> vertexPropertyIds = new TreeMap<>();
        db.schemaManager.populateVertexPropertySchemaMapToStringMap(vertexPropertyIdsDisk, vertexPropertyIds);
        graph.getIdFactory().convertMapToLazyIdsInPlace(vertexPropertyIds, graph, LazyVertexPropertyIdTransform.class);
        final Map<String, LazyIdTransform> fireflyVertexPropertyIds = (Map) vertexPropertyIds;
        return create(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds,
                fireflyVertexPropertyIds, vertexPropertyValues, vertexPropertyTypeHints, vertexPropertyProperties,
                vertexPropertyPropertiesTypeHints,
                edgeCacheOverflowed, db);
    }
}
