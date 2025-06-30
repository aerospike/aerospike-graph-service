package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.LazyEdgeCacheIdTransform;
import com.aerospike.firefly.structure.id.LazyIdTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FireflyVertexFactory {
    public static FireflyVertex create(final FireflyId fid,
                                       final String label,
                                       final FireflyGraph graph,
                                       final Map<String, List<LazyIdTransform>> inEdgeIds,
                                       final Map<String, List<LazyIdTransform>> outEdgeIds,
                                       final Map<Long, HashMap<Object, List<Long>>> vertexProperties,
                                       final Map<Long, Map<Long, Object>> vpTypeHints,
                                       final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties,
                                       final boolean isEdgeCacheOverflowed) {

        return new FireflyVertex(
                fid,
                label,
                graph,
                inEdgeIds,
                outEdgeIds,
                vertexProperties,
                vpTypeHints,
                vpProperties,
                isEdgeCacheOverflowed);
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

        // Read the Vertex's record from the database.
        if (record == null) {
            return null;
        }

        // Get ID and Label for Vertex.
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

        // Get Vertex Properties and VP Properties
        final Map<Long, HashMap<Object, List<Long>>> vertexProperties =
                (Map<Long, HashMap<Object, List<Long>>>) record.getMap(db.VERTEX_PROPERTY_DATA_BIN);
        final Map<Long, Map<Long, Object>> vpTypeHints =
                (Map<Long, Map<Long, Object>>) record.getMap(db.VERTEX_PROPERTY_TH_BIN);
        final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties = (Map) record.getMap(db.VP_PROPERTY_BIN);

        return create(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds,
                vertexProperties, vpTypeHints, vpProperties, edgeCacheOverflowed);
    }
}
