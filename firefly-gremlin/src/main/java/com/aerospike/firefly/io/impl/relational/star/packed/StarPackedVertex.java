package com.aerospike.firefly.io.impl.relational.star.packed;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertexProperty;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertexProperty;
import com.aerospike.firefly.io.impl.relational.star.StarVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarPackedVertex extends PackedVertex implements StarVertex {
    public static final int VERTEX_TYPE_HINT = 2;
    private static final Logger LOG = LoggerFactory.getLogger(StarPackedVertex.class);
    private final Map<String, Map<String, Object>> inVertexProperties;
    private final Map<String, Map<String, Object>> outVertexProperties;
    private final Map<String, Map<String, Long>> inVertexPropertiesIds;
    private final Map<String, Map<String, Long>> outVertexPropertiesIds;
    private final Map<String, Map<String, List<Long>>> inInEdges;
    private final Map<String, Map<String, List<Long>>> inOutEdges;
    private final Map<String, Map<String, List<Long>>> outInEdges;
    private final Map<String, Map<String, List<Long>>> outOutEdges;
    private final AerospikeConnection db;
    // IMPORTANT NOTE ON VERTEX PROPERTIES:
    //      StarPackedVertex currently does ONLY supports SINGLE cardinality.
    private Map<String, List<Long>> vertexPropertyIds;

    /**
     * Constructor for StarLinkedVertex.
     *
     * @param fid                 firefly id.
     * @param label               label.
     * @param graph               graph.
     * @param inEdgeIds           incoming edge ids - null if invalid (cache disabled or too many).
     * @param outEdgeIds          outgoing edge ids - null if invalid (cache disabled or too many).
     * @param inEdgeCount         incoming edge count.
     * @param outEdgeCount        outgoing edge count.
     * @param vertexPropertyIds   vertex property ids.
     * @param vertexPropertyCount vertex property count.
     * @param db                  Aerospike connection.
     */
    public StarPackedVertex(final FireflyId fid,
                            final String label,
                            final FireflyGraph graph,
                            final Map<String, List<Long>> inEdgeIds,
                            final Map<String, List<Long>> outEdgeIds,
                            final Map<String, Map<String, Object>> inVertexProperties,
                            final Map<String, Map<String, Object>> outVertexProperties,
                            final Map<String, Map<String, Long>> inVertexPropertiesIds,
                            final Map<String, Map<String, Long>> outVertexPropertiesIds,
                            final Map<String, Map<String, List<Long>>> inInEdges,
                            final Map<String, Map<String, List<Long>>> inOutEdges,
                            final Map<String, Map<String, List<Long>>> outInEdges,
                            final Map<String, Map<String, List<Long>>> outOutEdges,
                            final long inEdgeCount,
                            final long outEdgeCount,
                            final Map<String, Long> vertexPropertyIds,
                            final Map<String, Object> vertexPropertyValues,
                            final Map<String, Long> vertexPropertyValuesTypeHints,
                            final long vertexPropertyCount,
                            final AerospikeConnection db) {
        super(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount, vertexPropertyIds, vertexPropertyValues, vertexPropertyValuesTypeHints, vertexPropertyCount, db);
        this.inVertexProperties = inVertexProperties;
        this.outVertexProperties = outVertexProperties;
        this.inVertexPropertiesIds = inVertexPropertiesIds;
        this.outVertexPropertiesIds = outVertexPropertiesIds;
        this.inInEdges = inInEdges;
        this.inOutEdges = inOutEdges;
        this.outInEdges = outInEdges;
        this.outOutEdges = outOutEdges;
        this.db = db;
    }

    /**
     * Get edge ids for a pair of labels off of a supplied dirDirEdge map.
     *
     * @param dirDirEdgeIds Map of direction to direction to edge ids.
     * @param label1        First label. Null is any label.
     * @param label2        Second label. Null is any label.
     * @return Edge ids.
     */
    private List<Long> getEdgeIdsFromBothLabels(final Map<String, Map<String, List<Long>>> dirDirEdgeIds, final String label1, final String label2) {
        final List<Map<String, List<Long>>> list = new ArrayList<>();
        if (label1 == null) {
            // Get all edges for label1.
            list.addAll(dirDirEdgeIds.values());
        } else {
            // Get edges only for label1.
            list.add(dirDirEdgeIds.getOrDefault(label1, new HashMap<>()));
        }

        final List<Long> output = new ArrayList<>();
        if (label2 == null) {
            // Get all edges for all possible values of label2.
            list.forEach(dirEdgeIds -> dirEdgeIds.values().forEach(output::addAll));
        } else {
            // Get label2 specified edges.
            list.forEach(dirEdgeIds -> output.addAll(dirEdgeIds.getOrDefault(label2, new ArrayList<>())));
        }
        return output;
    }

    /**
     * Gets edges of adjacent vertices for the given directions and labels.
     *
     * @param direction1 Direction of the first edge.
     * @param direction2 Direction of the second edge.
     * @param label1     Label of the first edge. Set null for all labels.
     * @param label2     Label of the second edge. Set null for all labels.
     * @return List of long keys for edges of adjacent vertices.
     */
    public List<Long> getAdjacentVertexEdgeIds(final Direction direction1, final Direction direction2, final String label1, final String label2) {
        if (direction1 == Direction.IN) {
            // inX where X = direction2.
            if (direction2 == Direction.IN) {
                // inIn.
                return getEdgeIdsFromBothLabels(inInEdges, label1, label2);
            } else if (direction2 == Direction.OUT) {
                // inOut.
                return getEdgeIdsFromBothLabels(inOutEdges, label1, label2);
            } else {
                // inIn, inOut.
                final List<Long> inBothEdges = getEdgeIdsFromBothLabels(inInEdges, label1, label2);
                inBothEdges.addAll(getEdgeIdsFromBothLabels(inOutEdges, label1, label2));
                return inBothEdges;
            }
        } else if (direction1 == Direction.OUT) {
            // outX where X = direction2.
            if (direction2 == Direction.IN) {
                // outIn.
                return getEdgeIdsFromBothLabels(outInEdges, label1, label2);
            } else if (direction2 == Direction.OUT) {
                // outOut.
                return getEdgeIdsFromBothLabels(outOutEdges, label1, label2);
            } else {
                // outIn, outOut.
                final List<Long> outBothEdges = getEdgeIdsFromBothLabels(outInEdges, label1, label2);
                outBothEdges.addAll(getEdgeIdsFromBothLabels(outOutEdges, label1, label2));
                return outBothEdges;
            }
        } else {
            // inX, outX where X = direction2.
            if (direction2 == Direction.IN) {
                // inIn, outIn.
                final List<Long> bothInEdges = getEdgeIdsFromBothLabels(inInEdges, label1, label2);
                bothInEdges.addAll(getEdgeIdsFromBothLabels(outInEdges, label1, label2));
                return bothInEdges;
            } else if (direction2 == Direction.OUT) {
                // inOut, outOut.
                final List<Long> bothOutEdges = getEdgeIdsFromBothLabels(inOutEdges, label1, label2);
                bothOutEdges.addAll(getEdgeIdsFromBothLabels(outOutEdges, label1, label2));
                return bothOutEdges;
            } else {
                // inOut, outOut, inIn, outIn.
                final List<Long> bothBothEdges = getEdgeIdsFromBothLabels(inInEdges, label1, label2);
                bothBothEdges.addAll(getEdgeIdsFromBothLabels(inOutEdges, label1, label2));
                bothBothEdges.addAll(getEdgeIdsFromBothLabels(outInEdges, label1, label2));
                bothBothEdges.addAll(getEdgeIdsFromBothLabels(outOutEdges, label1, label2));
                return bothBothEdges;
            }
        }
    }

    public FireflyVertexProperty getAdjacentVertexProperty(final Direction direction, final String adjacentVertexLabel, final String vertexPropertyLabel) {
        final Object vertexProperty = ((direction == Direction.IN) ? inVertexProperties : outVertexProperties)
                .getOrDefault(adjacentVertexLabel, new HashMap<>()).getOrDefault(vertexPropertyLabel, null);
        return new PackedVertexProperty<>(graph, id, this, vertexPropertyLabel, vertexProperty);
    }
}
