package com.aerospike.firefly.io.impl.relational.star.packed;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertexProperty;
import com.aerospike.firefly.io.impl.relational.star.StarVertex;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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

    //
    // TODO: When we implement strategies these should become static functions that take the id of the vertex in
    //      question and find the information.
    //
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

    public static void writeAdjacentProperties(final AerospikeConnection db,
                                               final Direction direction,
                                               final FireflyVertex vertex,
                                               final FireflyVertex adjacentVertex,
                                               final String label) {
        LOG.debug("Writing adjacent properties. Vertex: {}, adjacentVertex: {}.", vertex.id(), adjacentVertex.id());
        final String set = direction.equals(Direction.IN) ? db.IN_VP_SET : db.OUT_VP_SET;

        // We need to be careful here because order matters.
        FireflyRecord record = FireflyRecord.read(db, set, vertex.id);

        // Read the ids, values, and type hints from the record.
        Map<String, List<Map<String, Long>>> vertexPropertyIds;
        Map<String, List<Map<String, Object>>> vertexPropertyValues;
        Map<String, List<Map<String, Long>>> vertexPropertyTypeHints;
        if (record == null) {
            // First entry, generate empty map.
            vertexPropertyIds = new HashMap<>();
            vertexPropertyValues = new HashMap<>();
            vertexPropertyTypeHints = new HashMap<>();
        } else {
            // Read existing maps.
            vertexPropertyIds = (Map<String, List<Map<String, Long>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
            vertexPropertyValues = (Map<String, List<Map<String, Object>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE);
            vertexPropertyTypeHints = (Map<String, List<Map<String, Long>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);

            // If any bins happen to be null, initialize them.
            if (vertexPropertyIds == null) {
                vertexPropertyIds = new HashMap<>();
            }
            if (vertexPropertyValues == null) {
                vertexPropertyValues = new HashMap<>();
            }
            if (vertexPropertyTypeHints == null) {
                vertexPropertyTypeHints = new HashMap<>();
            }
        }

        // If there is no entry for the label, create an empty list there.
        if (!vertexPropertyIds.containsKey(label)) {
            vertexPropertyIds.put(label, new ArrayList<>());
            vertexPropertyValues.put(label, new ArrayList<>());
            vertexPropertyTypeHints.put(label, new ArrayList<>());
        }

        // Create a map for the id, value, and type hint.
        final Map<String, Long> vertexPropertyIdMap = new HashMap<>();
        final Map<String, Object> vertexPropertyValueMap = new HashMap<>();
        final Map<String, Long> vertexPropertyTypeHintMap = new HashMap<>();
        adjacentVertex.properties().forEachRemaining(vp -> {
            vertexPropertyIdMap.put(vp.key(), NumericIdManager.convert(vp.id()));
            vertexPropertyValueMap.put(vp.key(), vp.value());
            vertexPropertyTypeHintMap.put(vp.key(), db.getSupportedType(vp.value().getClass()));
        });

        // Add maps to lists.
        vertexPropertyIds.get(label).add(vertexPropertyIdMap);
        vertexPropertyValues.get(label).add(vertexPropertyValueMap);
        vertexPropertyTypeHints.get(label).add(vertexPropertyTypeHintMap);

        // Create bins for the maps.
        final Bin vertexPropertyIdMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexPropertyIds));
        final Bin vertexPropertyValueMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(vertexPropertyValues));
        final Bin vertexPropertyTypeHintMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, Value.get(vertexPropertyTypeHints));

        // Write element. This isn't really an element, but the logic holds.
        FireflyRecord.writeElement(db, set, vertex.id, vertexPropertyIdMapBin, vertexPropertyValueMapBin, vertexPropertyTypeHintMapBin);
    }

    public static void writeBirectionalEdgesToAdjacentVertices(final AerospikeConnection db,
                                                               final Direction direction,
                                                               final FireflyVertex vertex,
                                                               final FireflyId edgeId,
                                                               final String label) {
        // Let's consider an example with the following notation:
        // vertexId1-edgeId>vertexId2
        //
        // Start with the following:
        //  A-1>B
        //  A-2>C
        //  A-3>D
        //  B-4>C
        //
        // If we want to now add:
        //  B-5>C
        // We must go to A and append on the compounding edge of A-1>B
        // We must also go to C and append on the compounding edge of C-4>B
        //
        // Thinking about that programmatically, we need to loop through the edges of B and add
        // a compounding edge to each one.
        //
        // The storage format of the compounding edge is:
        // { edgeIdLabel : [ { adjacentEdgeLabel : [ adjacentEdgeId ] } ] }
        //
        // Lets further assume all our edges have the same label of "X"
        // So in our example we'd have the following for A's out().out() edges:
        // { "X" : [ { "X" : [4] } ] }
        // The reason we have the above is that only B has an outbound edge.
        //
        // If we added B-5>C, we'd have the following:
        // { "X" : [ { "X" : [4, 5] } ] }
        //
        // If we instead added C-5>B, we'd have the following:
        // { "X" : [ { "X" : [4] }, { "X" : [5] } ] }
        //
        // The key takeaway here being that we have an ordered list of adjacent edges to A.
        // The order MUST match the order of the edges in the vertex record, stored as:
        // { "X" : [1, 2, 3] }
        // Otherwise we cannot reverse engineer information on our pathing using the vertex record and the

        // Go through in edges first.
        Iterator<Edge> edges = vertex.edges(Direction.IN);

        // This set is in regard to the adjacent vertex so flip initial IN to OUT.
        final String finalAdjacentVertexDirDirSet1 = (direction.equals(Direction.IN) ? db.OUT_IN_SET : db.OUT_OUT_SET);
        edges.forEachRemaining(edge -> {
            // If the edge is `this` edge, skip it. It will cause duplicated data otherwise.
            if (!edge.id().equals(edgeId.value())) {
                final FireflyId adjacentVertexId = ((FireflyEdge) edge).outVertexId();
                LOG.debug("Writing vertex {} adjacent vertex {} compound edge {} from edge {}", vertex.id(), adjacentVertexId.value(), edge.id(), edgeId.value());

                // We need to be careful here because order matters.
                final FireflyRecord adjacentVertexDirDirRecord = FireflyRecord.read(db, finalAdjacentVertexDirDirSet1, adjacentVertexId);
                final FireflyRecord adjacentVertexRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, adjacentVertexId);

                // Get adjacent vertex records edge map.
                final Map<String, List<Long>> adjacentVertexEdges;
                if (adjacentVertexRecord == null) {
                    // First entry, generate empty map.
                    adjacentVertexEdges = new HashMap<>();
                } else {
                    final String adjacentVertexDirBin = db.OUT_EDGES;
                    adjacentVertexEdges = (Map<String, List<Long>>) adjacentVertexRecord.record.getMap(adjacentVertexDirBin);

                    // If the bin is null it must be initialized.
                    if (adjacentVertexEdges == null) {
                        // This should never happen.
                        LOG.error("Bin not found in adjacent vertex {} {}", adjacentVertexId, adjacentVertexDirBin);
                        throw new RuntimeException(String.format("Bin not found in adjacent vertex %s %s", adjacentVertexId, adjacentVertexDirBin));
                    }
                }

                final String edgeLabel = edge.label();
                if (!adjacentVertexEdges.containsKey(edge.label())) {
                    // This should never happen.
                    LOG.error("Edge label {} not found in adjacent vertex {}", edgeLabel, adjacentVertexId);
                    throw new RuntimeException(String.format("Edge label %s not found in adjacent vertex %s", edgeLabel, adjacentVertexId));
                }

                final List<Long> edgeIds = adjacentVertexEdges.get(edgeLabel);
                final int edgeIndex = findInList(edgeIds, NumericIdManager.convert(edge.id()),
                        String.format("Failed to find edge id %s in edge set %s of map %s", edge.id(), edgeIds, adjacentVertexEdges));

                // Get the adjacent edge map.
                Map<String, List<Map<String, List<Long>>>> adjacentEdges;
                if (adjacentVertexDirDirRecord == null) {
                    // First entry, generate empty map.
                    adjacentEdges = new HashMap<>();
                } else {
                    adjacentEdges = (Map<String, List<Map<String, List<Long>>>>) adjacentVertexDirDirRecord.record.getMap(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN);

                    // If the bin is null it must be initialized.
                    if (adjacentEdges == null) {
                        adjacentEdges = new HashMap<>();
                    }
                }
                List<Map<String, List<Long>>> adjacentVertexEdgeLabelToEdgeIds = adjacentEdges.get(edge.label());
                if (adjacentVertexEdgeLabelToEdgeIds == null) {
                    // This should never happen.
                    adjacentVertexEdgeLabelToEdgeIds = new ArrayList<>();
                }

                if (adjacentVertexEdgeLabelToEdgeIds.size() != edgeIds.size()) {
                    // This should never happen.
                    LOG.error("Mismatch in adjacent vertex edge sets of edge {} and vertex {}: {} {}", edgeLabel, adjacentVertexId, adjacentVertexEdgeLabelToEdgeIds, edgeIds);
                    throw new RuntimeException(String.format("Mismatch in adjacent vertex edge %s and vertex %s: %s %s", edgeLabel, adjacentVertexId, adjacentVertexEdgeLabelToEdgeIds, edgeIds));
                }

                final Map<String, List<Long>> innerEdgeMap = adjacentVertexEdgeLabelToEdgeIds.get(edgeIndex);
                if (!innerEdgeMap.containsKey(label)) {
                    innerEdgeMap.put(label, new ArrayList<>());
                }
                innerEdgeMap.get(label).add(NumericIdManager.convert(edgeId.value()));
                adjacentVertexEdgeLabelToEdgeIds.set(edgeIndex, innerEdgeMap);
                adjacentEdges.put(edge.label(), adjacentVertexEdgeLabelToEdgeIds);

                final Bin bin = new Bin(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, Value.get(adjacentEdges));
                FireflyRecord.writeElement(db, finalAdjacentVertexDirDirSet1, adjacentVertexId, bin);
            }
        });

        // Now go through out edges.
        edges = vertex.edges(Direction.OUT);

        // This set is out regard to the adjacent vertex so flip initial OUT to IN.
        final String adjacentVertexDirDirSet2 = (direction.equals(Direction.IN) ? db.IN_IN_SET : db.IN_OUT_SET);
        edges.forEachRemaining(edge -> {
            // If the edge is `this` edge, skip it. It will cause duplicated data otherwise.
            if (!edge.id().equals(edgeId.value())) {
                final FireflyId adjacentVertexId = ((FireflyEdge) edge).inVertexId();
                LOG.debug("Writing vertex {} adjacent vertex compound edge {} from edge {}", adjacentVertexId.value(), edge.id(), edgeId.value());

                // We need to be careful here because order matters.
                final FireflyRecord adjacentVertexDirDirRecord = FireflyRecord.read(db, adjacentVertexDirDirSet2, adjacentVertexId);
                final FireflyRecord adjacentVertexRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, adjacentVertexId);

                // Get adjacent vertex records edge map.
                final Map<String, List<Long>> adjacentVertexEdges;
                if (adjacentVertexRecord == null) {
                    // First entry, generate empty map.
                    adjacentVertexEdges = new HashMap<>();
                } else {
                    final String adjacentVertexDirBin = db.IN_EDGES;
                    adjacentVertexEdges = (Map<String, List<Long>>) adjacentVertexRecord.record.getMap(adjacentVertexDirBin);

                    // If the bin is null it must be initialized.
                    if (adjacentVertexEdges == null) {
                        // This should never happen.
                        LOG.error("Bin not found in adjacent vertex {} {}", adjacentVertexId, adjacentVertexDirBin);
                        throw new RuntimeException(String.format("Bin not found in adjacent vertex %s %s", adjacentVertexId, adjacentVertexDirBin));
                    }
                }

                final String edgeLabel = edge.label();
                if (!adjacentVertexEdges.containsKey(edge.label())) {
                    // This should never happen.
                    LOG.error("Edge label {} not found in adjacent vertex {}", edgeLabel, adjacentVertexId);
                    throw new RuntimeException(String.format("Edge label %s not found in adjacent vertex %s", edgeLabel, adjacentVertexId));
                }

                final List<Long> edgeIds = adjacentVertexEdges.get(edgeLabel);
                final int edgeIndex = findInList(edgeIds, NumericIdManager.convert(edge.id()),
                        String.format("Failed to find edge id %s in edge set %s of map %s", edge.id(), edgeIds, adjacentVertexEdges));

                // Get the adjacent edge map.
                Map<String, List<Map<String, List<Long>>>> adjacentEdges;
                if (adjacentVertexDirDirRecord == null) {
                    // First entry, generate empty map.
                    adjacentEdges = new HashMap<>();
                } else {
                    adjacentEdges = (Map<String, List<Map<String, List<Long>>>>) adjacentVertexDirDirRecord.record.getMap(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN);

                    // If the bin is null it must be initialized.
                    if (adjacentEdges == null) {
                        adjacentEdges = new HashMap<>();
                    }
                }
                List<Map<String, List<Long>>> adjacentVertexEdgeLabelToEdgeIds = adjacentEdges.get(edge.label());
                if (adjacentVertexEdgeLabelToEdgeIds == null) {
                    // This should never happen.
                    adjacentVertexEdgeLabelToEdgeIds = new ArrayList<>();
                }

                if (adjacentVertexEdgeLabelToEdgeIds.size() != edgeIds.size()) {
                    // This should never happen.
                    LOG.error("Mismatch in adjacent vertex edge sets of edge {} and vertex {}: {} {}", edgeLabel, adjacentVertexId, adjacentVertexEdgeLabelToEdgeIds, edgeIds);
                    throw new RuntimeException(String.format("Mismatch in adjacent vertex edge %s and vertex %s: %s %s", edgeLabel, adjacentVertexId, adjacentVertexEdgeLabelToEdgeIds, edgeIds));
                }

                final Map<String, List<Long>> innerEdgeMap = adjacentVertexEdgeLabelToEdgeIds.get(edgeIndex);
                if (!innerEdgeMap.containsKey(label)) {
                    innerEdgeMap.put(label, new ArrayList<>());
                }
                innerEdgeMap.get(label).add(NumericIdManager.convert(edgeId.value()));
                adjacentVertexEdgeLabelToEdgeIds.set(edgeIndex, innerEdgeMap);
                adjacentEdges.put(edge.label(), adjacentVertexEdgeLabelToEdgeIds);

                final Bin bin = new Bin(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, Value.get(adjacentEdges));
                FireflyRecord.writeElement(db, adjacentVertexDirDirSet2, adjacentVertexId, bin);
            }
        });
    }

    private static int findInList(final List<Long> list, final Long value, final String errorMessage) {
        int edgeIndex = -1;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).equals(value)) {
                edgeIndex = i;
                break;
            }
        }
        if (edgeIndex == -1) {
            // This should never happen.
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        return edgeIndex;
    }

    public static void writeCompoundEdgesOnNewEdge(final AerospikeConnection db, final FireflyVertex vertex, final Direction direction, final String edgeLabel, final FireflyVertex adjacentVertex) {
        // This is implicitly ordered due to the fact that we do it in series. This means we don't have to deal with the
        // complexity of ordering the compound edges.

        // We need to get both sets that need to be updated. The sets are <direction>In and <direction>Out.
        final String dirInSet = direction.equals(Direction.IN) ? db.IN_IN_SET : db.OUT_IN_SET;
        final String dirOutSet = direction.equals(Direction.IN) ? db.IN_OUT_SET : db.OUT_OUT_SET;

        // Get this vertices current edgeLabel->list of edgeLabel->edgeIds map.
        final Map<String, List<Map<String, List<Long>>>> vertexInDirEdgeMap = getOrDefaultHashMap(db, dirInSet, vertex.id, db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN);
        final Map<String, List<Map<String, List<Long>>>> vertexOutDirEdgeMap = getOrDefaultHashMap(db, dirOutSet, vertex.id, db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN);

        // This adjacent vertex edgeLabel->edgeIds map.
        final Map<String, List<Long>> adjacentVertexInEdgeMap = getOrDefaultHashMap(db, db.VERTEX_AERO_SET, adjacentVertex.id, db.IN_EDGES);
        final Map<String, List<Long>> adjacentVertexOutEdgeMap = getOrDefaultHashMap(db, db.VERTEX_AERO_SET, adjacentVertex.id, db.OUT_EDGES);

        // Insert into maps list again implicit ordering is on our side here so we don't need to do any checks.
        // Get in map of edge to edge lists, add the new in edge map, and insert it back.
        final List<Map<String, List<Long>>> inEdgeMapList = vertexInDirEdgeMap.getOrDefault(edgeLabel, new ArrayList<>());
        inEdgeMapList.add(adjacentVertexInEdgeMap);
        vertexInDirEdgeMap.put(edgeLabel, inEdgeMapList);

        // Get out map of edge to edge lists, add the new out edge map, and insert it back.
        final List<Map<String, List<Long>>> outEdgeMapList = vertexOutDirEdgeMap.getOrDefault(edgeLabel, new ArrayList<>());
        outEdgeMapList.add(adjacentVertexOutEdgeMap);
        vertexOutDirEdgeMap.put(edgeLabel, outEdgeMapList);

        // Create bins for the new maps.Typi
        final Bin inBin = new Bin(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, Value.get(vertexInDirEdgeMap));
        final Bin outBin = new Bin(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, Value.get(vertexOutDirEdgeMap));

        // Insert in Aerospike.
        LOG.debug("Writing compound edges for vertex {} on sets {}={} & {}={}", vertex.id, dirInSet, vertexInDirEdgeMap, dirOutSet, vertexOutDirEdgeMap);
        FireflyRecord.writeElement(db, dirInSet, vertex.id, inBin);
        FireflyRecord.writeElement(db, dirOutSet, vertex.id, outBin);
    }

    public static void writeVertexPropertyToAdjacentVertices(final AerospikeConnection db,
                                                             final FireflyVertex vertex,
                                                             final FireflyVertexProperty<?> fireflyVertexProperty) {

        addVertexPropertyAdjacentVertexViaEdges(db, Direction.IN, vertex, fireflyVertexProperty);
        addVertexPropertyAdjacentVertexViaEdges(db, Direction.OUT, vertex, fireflyVertexProperty);
    }

    private static void addVertexPropertyAdjacentVertexViaEdges(final AerospikeConnection db,
                                                                final Direction direction,
                                                                final FireflyVertex vertex,
                                                                final FireflyVertexProperty<?> fireflyVertexProperty) {
        // We need to loop through attached vertices, getting their edge in the opposite direction (which points at
        // this vertex) and then add the property.
        Iterator<Edge> edges = vertex.edges(direction);

        // Set is switched since this is running on adjacent vertex.
        final String set = direction.equals(Direction.IN) ? db.OUT_VP_SET : db.IN_VP_SET;
        edges.forEachRemaining(edge -> {
            // Need to get vp of edge in opposite direction (which points from the adjacent vertex to this vertex).
            final FireflyId vpId = (direction == Direction.IN) ? ((FireflyEdge) edge).outVertexId() : ((FireflyEdge) edge).inVertexId();

            // Get all vertex property related bins.
            final FireflyRecord record = FireflyRecord.read(db, set, vpId);
            if (record == null) {
                LOG.error("Could not find {} record for vertex {} when adding properties to adjacent lists of vertex {}.", set, vpId, vertex.id());
                throw new RuntimeException(String.format("Could not find %s record for vertex %s when adding properties to adjacent lists of vertex %s.", set, vpId, vertex.id()));

            }
            final Map<String, List<Map<String, Object>>> vertexVPValue = (Map<String, List<Map<String, Object>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE);
            final Map<String, List<Map<String, Long>>> vertexVPTypeHint = (Map<String, List<Map<String, Long>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
            final Map<String, List<Map<String, Long>>> vertexVPId = (Map<String, List<Map<String, Long>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);


            if (vertexVPValue == null || !vertexVPValue.containsKey(edge.label()) ||
                    vertexVPTypeHint == null || !vertexVPTypeHint.containsKey(edge.label()) ||
                    vertexVPId == null || !vertexVPId.containsKey(edge.label())) {
                LOG.error("Vertex property list {} of vertex {} were missing edge label {} when adding properties to adjacent lists of vertex {}.", set, vpId, edge.label(), vertex.id());
                throw new RuntimeException(String.format("Vertex property list %s of vertex %s were missing edge label %s when adding properties to adjacent lists of vertex %s.", set, vpId, edge.label(), vertex.id()));
            }

            // To get our entry, we need to find which position this edge exists in in the master list of the vertex.
            final Map<String, List<Long>> edgeLabelToId = getOrDefaultHashMap(db, db.VERTEX_AERO_SET, vertex.id, db.OUT_EDGES);
            if (!edgeLabelToId.containsKey(edge.label())) {
                LOG.error("Could not find edge label {} in vertex {} when adding properties to adjacent lists of vertex {}.", edge.label(), vertex.id(), vpId);
                throw new RuntimeException(String.format("Could not find edge label %s in vertex %s when adding properties to adjacent lists of vertex %s.", edge.label(), vertex.id(), vpId));
            }

            List<Long> edgeIds = edgeLabelToId.get(edge.label());
            List<Map<String, Object>> vertexPropertyValues = vertexVPValue.get(edge.label());
            List<Map<String, Long>> vertexPropertyTypeHints = vertexVPTypeHint.get(edge.label());
            List<Map<String, Long>> vertexPropertyIds = vertexVPId.get(edge.label());
            if (edgeIds.size() != vertexPropertyValues.size()) {
                LOG.error("Edge list size {} did not match vertex property list size {} for vertex {} when adding properties to adjacent lists of vertex {}.", edgeIds.size(), vertexPropertyValues.size(), vpId, vertex.id());
                throw new RuntimeException(String.format("Edge list size %s did not match vertex property list size %s for vertex %s when adding properties to adjacent lists of vertex %s.", edgeIds.size(), vertexPropertyValues.size(), vpId, vertex.id()));
            }
            final int edgeIndex = findInList(edgeIds, NumericIdManager.convert(edge.id()),
                    String.format("Failed to find edge %s in vertex %s when adding properties to adjacent lists of vertex %s.", edge, vpId, vertex.id()));
            vertexPropertyValues.get(edgeIndex).put(fireflyVertexProperty.key(), fireflyVertexProperty.value());
            vertexVPValue.put(edge.label(), vertexPropertyValues);
            vertexPropertyTypeHints.get(edgeIndex).put(fireflyVertexProperty.key(), db.getSupportedType(fireflyVertexProperty.value().getClass()));
            vertexVPTypeHint.put(edge.label(), vertexPropertyTypeHints);
            vertexPropertyIds.get(edgeIndex).put(fireflyVertexProperty.key(), NumericIdManager.convert(fireflyVertexProperty.id()));
            vertexVPId.put(edge.label(), vertexPropertyIds);

            // Create bins for the maps.
            final Bin vertexPropertyIdMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexPropertyIds));
            final Bin vertexPropertyValueMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(vertexPropertyValues));
            final Bin vertexPropertyTypeHintMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, Value.get(vertexPropertyTypeHints));

            // Write element. This isn't really an element, but the logic holds.
            FireflyRecord.writeElement(db, set, vertex.id, vertexPropertyIdMapBin, vertexPropertyValueMapBin, vertexPropertyTypeHintMapBin);
        });
    }

    // TODO: Use this in other places.
    private static <K, U> Map<K, U> getOrDefaultHashMap(final AerospikeConnection db, final String set, final FireflyId id, final String bin) {
        final FireflyRecord record = FireflyRecord.read(db, set, id);
        if (record == null) {
            return new HashMap<>();
        }
        final Map<K, U> map = (Map<K, U>) record.record.getMap(bin);
        return (map == null) ? new HashMap<>() : map;
    }
}
