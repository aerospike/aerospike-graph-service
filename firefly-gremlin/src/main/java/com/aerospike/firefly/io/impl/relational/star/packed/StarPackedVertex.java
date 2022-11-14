package com.aerospike.firefly.io.impl.relational.star.packed;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class StarPackedVertex extends PackedVertex {
    public static final int VERTEX_TYPE_HINT = 2;
    private static final Logger LOG = LoggerFactory.getLogger(StarPackedVertex.class);

    public StarPackedVertex(final FireflyId fid,
                            final String label,
                            final FireflyGraph graph,
                            final Map<String, List<FireflyId>> inEdgeIds,
                            final Map<String, List<FireflyId>> outEdgeIds,
                            final long inEdgeCount,
                            final long outEdgeCount,
                            final Map<String, FireflyId> vertexPropertyIds,
                            final Map<String, Object> vertexPropertyValues,
                            final Map<String, Long> vertexPropertyValuesTypeHints,
                            final long vertexPropertyCount,
                            final AerospikeConnection db) {
        super(fid, label, graph, inEdgeIds, outEdgeIds, inEdgeCount, outEdgeCount, vertexPropertyIds,
                vertexPropertyValues, vertexPropertyValuesTypeHints, vertexPropertyCount, db);
    }

    /**
     * Remove vertex. Any edges attached to a vertex must be removed
     * when the edge is removed.
     */
    @Override
    public void remove() {
        super.remove();

        // The star data model holds some additional data that must be removed when the vertex is removed.

        // To remove the vertex, we must simply remove the additional sets that are associated with the vertex.
        // The edge addition / removal is what builds and tears down the additional data in the sets,
        // so we don't need any fancy logic for that here.

        // Create list of all relevant sets, would be nice to make this static but unfortunately there are runtime
        // additions.
        final List<String> sets = List.of(this.db.IN_IN_SET, this.db.IN_OUT_SET, this.db.OUT_IN_SET,
                this.db.OUT_OUT_SET, this.db.IN_VP_SET, this.db.OUT_VP_SET);

        // Remove vertex from the list.
        LOG.debug("Removing vertex {} from sets: {}.", this.id.toString(), sets);
        sets.forEach(set ->
                this.db.delete(FireflyRecord.getKey(this.db.getNamespace(), this.db.VERTEX_AERO_SET, this.id)));
    }

    /**
     * Remove the vertex property from the vertex.
     *
     * @param key              vertex property label.
     * @param vertexPropertyId id of the vertex property to remove.
     */
    @Override
    public void removeVertexPropertyForModel(final String key, final FireflyId vertexPropertyId) {
        final StarPackedGraph graph = (StarPackedGraph) this.graph;
        removeVertexPropertyFromAdjacentVertices(key, graph.enableInVp, graph.enableOutVp);
        super.removeVertexPropertyForModel(key, vertexPropertyId);
    }

    public static void writeAdjacentProperties(final AerospikeConnection db,
                                               final Direction direction,
                                               final FireflyVertex vertex,
                                               final FireflyVertex adjacentVertex,
                                               final String label) {
        LOG.trace("Writing adjacent properties. Vertex: {}, adjacentVertex: {}.", vertex.id, adjacentVertex.id);
        final String set = direction.equals(Direction.IN) ? db.IN_VP_SET : db.OUT_VP_SET;

        // We need to be careful here because order matters.
        FireflyRecord record = FireflyRecord.read(db, set, vertex.id);

        // Read the ids, values, and type hints from the record.
        Map<String, List<Map<String, Object>>> vertexPropertyIds;
        Map<String, List<Map<String, Object>>> vertexPropertyValues;
        Map<String, List<Map<String, Long>>> vertexPropertyTypeHints;
        if (record == null) {
            // First entry, generate empty map.
            vertexPropertyIds = new HashMap<>();
            vertexPropertyValues = new HashMap<>();
            vertexPropertyTypeHints = new HashMap<>();
        } else {
            // Read existing maps.
            vertexPropertyIds = (Map<String, List<Map<String, Object>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
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
        final Map<String, Object> vertexPropertyIdMap = new HashMap<>();
        final Map<String, Object> vertexPropertyValueMap = new HashMap<>();
        final Map<String, Long> vertexPropertyTypeHintMap = new HashMap<>();
        adjacentVertex.properties().forEachRemaining(vp -> {
            vertexPropertyIdMap.put(vp.key(), ((FireflyVertexProperty)vp).id.getStorageId());
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
        FireflyRecord.writeElement(db, set, vertex.id, -1, vertexPropertyIdMapBin, vertexPropertyValueMapBin, vertexPropertyTypeHintMapBin);
    }

    public static void writeBidirectionalEdgesToAdjacentVertices(final AerospikeConnection db,
                                                                 final FireflyVertex inVertex,
                                                                 final FireflyVertex outVertex,
                                                                 final FireflyId edgeId,
                                                                 final String label,
                                                                 final Direction direction,
                                                                 final Direction secondaryDirection) {
        // Let's consider an example with the following notation:
        // vertexId1-edge>vertexId2
        //
        // Start with the following:
        //  A->B
        //  C->D
        //
        // If we want to now add:
        //  B->C
        // We must go to A and append on the compounding edge of B->C
        // We must also go to C and append on the compounding edge of B-4>C
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
        final Iterator<Edge> edges;
        final String dirDirSet;
        final Vertex vertex;

        // This set is in regard to the adjacent vertex so flip initial IN to OUT on vertex edge grab.
        // We also need to flip the in/out initial direction to get the appropriate vertex.
        if (direction.equals(Direction.IN)) {
            vertex = inVertex;
            if (secondaryDirection.equals(Direction.IN)) {
                dirDirSet = db.IN_IN_SET;
                edges = inVertex.edges(Direction.OUT);
            } else {
                dirDirSet = db.IN_OUT_SET;
                edges = outVertex.edges(Direction.OUT);
            }
        } else {
            vertex = outVertex;
            if (secondaryDirection.equals(Direction.IN)) {
                dirDirSet = db.OUT_IN_SET;
                edges = inVertex.edges(Direction.IN);
            } else {
                dirDirSet = db.OUT_OUT_SET;
                edges = outVertex.edges(Direction.IN);
            }
        }

        edges.forEachRemaining(edge -> {
            // If the edge is `this` edge, skip it. It will cause duplicated data otherwise.
            if (!((FireflyEdge)edge).id.equals(edgeId)) {
                final FireflyId adjacentVertexId = direction.equals(Direction.IN) ? ((FireflyEdge) edge).inVertexId() : ((FireflyEdge) edge).outVertexId();
                LOG.trace("Writing vertex {} adjacent vertex {} compound edge {} from edge {}", vertex.id(), adjacentVertexId, edge.id(), edgeId);

                // We need to be careful here because order matters.
                final FireflyRecord adjacentVertexRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, adjacentVertexId);

                // Get adjacent vertex records edge map.
                final Map<String, List<Object>> adjacentVertexEdges;
                if (adjacentVertexRecord == null) {
                    // First entry, generate empty map.
                    adjacentVertexEdges = new HashMap<>();
                } else {
                    final String adjacentVertexDirBin = direction.equals(Direction.IN) ? db.IN_EDGES : db.OUT_EDGES;
                    adjacentVertexEdges = (Map<String, List<Object>>) adjacentVertexRecord.record.getMap(adjacentVertexDirBin);

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

                final List<Object> edgeIds = adjacentVertexEdges.get(edgeLabel);
                final int edgeIndex = findInList(edgeIds, ((FireflyEdge) edge).id.getStorageId(),
                                                 String.format("Failed to find edge id %s in edge set %s of map %s", edge.id(), edgeIds, adjacentVertexEdges));

                // Get the adjacent edge map.
                Map<String, List<Map<String, List<Object>>>> adjacentEdges;
                final FireflyRecord adjacentVertexDirDirRecord = FireflyRecord.read(db, dirDirSet, adjacentVertexId);
                if (adjacentVertexDirDirRecord == null) {
                    // First entry, generate empty map.
                    adjacentEdges = new HashMap<>();
                } else {
                    adjacentEdges = (Map<String, List<Map<String, List<Object>>>>) adjacentVertexDirDirRecord.record.getMap(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN);

                    // If the bin is null it must be initialized.
                    if (adjacentEdges == null) {
                        adjacentEdges = new HashMap<>();
                    }
                }
                List<Map<String, List<Object>>> adjacentVertexEdgeLabelToEdgeIds = adjacentEdges.get(edge.label());
                if (adjacentVertexEdgeLabelToEdgeIds == null) {
                    // This should never happen.
                    adjacentVertexEdgeLabelToEdgeIds = new ArrayList<>();
                }

                if (adjacentVertexEdgeLabelToEdgeIds.size() != edgeIds.size()) {
                    // This should never happen.
                    LOG.error("Mismatch in adjacent vertex edge sets of edge {} and vertex {}: {} {}", edgeLabel, adjacentVertexId, adjacentVertexEdgeLabelToEdgeIds, edgeIds);
                    throw new RuntimeException(String.format("Mismatch in adjacent vertex edge %s and vertex %s: %s %s", edgeLabel, adjacentVertexId, adjacentVertexEdgeLabelToEdgeIds, edgeIds));
                }

                final Map<String, List<Object>> innerEdgeMap = adjacentVertexEdgeLabelToEdgeIds.get(edgeIndex);
                if (!innerEdgeMap.containsKey(label)) {
                    innerEdgeMap.put(label, new ArrayList<>());
                }
                innerEdgeMap.get(label).add(edgeId.getStorageId());
                adjacentVertexEdgeLabelToEdgeIds.set(edgeIndex, innerEdgeMap);
                adjacentEdges.put(edge.label(), adjacentVertexEdgeLabelToEdgeIds);

                LOG.error("Writing bidirection edge for vertex {} on set {}={}", adjacentVertexId, dirDirSet, adjacentEdges);
                final Bin bin = new Bin(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, Value.get(adjacentEdges));
                FireflyRecord.writeElement(db, dirDirSet, adjacentVertexId, -1, bin);
            }
        });
    }

    private static int findInList(final List<Object> list,
                                  final Object value,
                                  final String errorMessage,
                                  final Object... args) {
        int edgeIndex = -1;
        for (int i = 0; i < list.size(); i++) {
            Object v;
            if (list.get(i) instanceof byte[]) {
                v = FireflyIdFactory.createId(list.get(i)).getStorageId();
            } else {
                v = list.get(i);
            }
            if (v.equals(value)) {
                edgeIndex = i;
                break;
            }
        }
        if (edgeIndex == -1) {
            // This should never happen.
            final String formattedErrorMessage = String.format(errorMessage, args);
            LOG.error(formattedErrorMessage);
            throw new RuntimeException(formattedErrorMessage);
        }
        return edgeIndex;
    }


    public static void writeCompoundEdgesOnNewEdge(final AerospikeConnection db,
                                                   final String edgeLabel,
                                                   final FireflyVertex inVertex,
                                                   final FireflyVertex outVertex,
                                                   final Direction direction,
                                                   final Direction secondaryDirection) {
        final FireflyId id;
        final Bin bin;
        final String dirSet;
        final String edgeDir;
        final FireflyId adjacentId;
        if (direction.equals(Direction.IN)) {
            id = inVertex.id;
            adjacentId = outVertex.id;
            if (secondaryDirection.equals(Direction.IN)) {
                dirSet = db.IN_IN_SET;
                edgeDir = db.IN_EDGES;
            } else {
                dirSet = db.IN_OUT_SET;
                edgeDir = db.OUT_EDGES;
            }
        } else {
            id = outVertex.id;
            adjacentId = inVertex.id;
            if (secondaryDirection.equals(Direction.IN)) {
                dirSet = db.OUT_IN_SET;
                edgeDir = db.IN_EDGES;
            } else {
                dirSet = db.OUT_OUT_SET;
                edgeDir = db.OUT_EDGES;
            }
        }

        // Get this vertices current edgeLabel->list of edgeLabel->edgeIds map.
        final Map<String, List<Map<String, List<Object>>>> vertexDirEdgeMap = getOrDefaultHashMap(db, dirSet, id, db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN);

        // This adjacent vertex edgeLabel->edgeIds map.
        final Map<String, List<Object>> adjacentVertexEdgeMap = getOrDefaultHashMap(db, db.VERTEX_AERO_SET, adjacentId, edgeDir);

        // Insert into maps list again implicit ordering is on our side here, so we don't need to do any checks.
        // Get in map of edge to edge lists, add the new in edge map, and insert it back.
        final List<Map<String, List<Object>>> edgeMapList = vertexDirEdgeMap.getOrDefault(edgeLabel, new ArrayList<>());
        edgeMapList.add(adjacentVertexEdgeMap);
        vertexDirEdgeMap.put(edgeLabel, edgeMapList);

        // Create bin for the new map and insert in Aerospike.
        LOG.trace("Writing compound edges for vertex {} on set {}={}", id, dirSet, vertexDirEdgeMap);
        bin = new Bin(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, Value.get(vertexDirEdgeMap));
        FireflyRecord.writeElement(db, dirSet, id, -1, bin);
    }

    public static void writeVertexPropertyToAdjacentVertices(final AerospikeConnection db,
                                                             final FireflyVertex vertex,
                                                             final FireflyVertexProperty<?> fireflyVertexProperty,
                                                             final boolean enableInVp,
                                                             final boolean enableOutVp) {
        // These are intentionally flipped.
        if (enableInVp) {
            addVertexPropertyAdjacentVertexViaEdges(db, Direction.OUT, vertex, fireflyVertexProperty);
        }
        if (enableOutVp) {
            addVertexPropertyAdjacentVertexViaEdges(db, Direction.IN, vertex, fireflyVertexProperty);
        }
    }

    private void removeVertexPropertyFromAdjacentVertices(final String key, final boolean enableInVp,
                                                          final boolean enableOutVp) {
        // These are intentionally flipped.
        if (enableInVp) {
            removeVertexPropertyAdjacentVertexViaEdges(Direction.OUT, key);
        }
        if (enableOutVp) {
            removeVertexPropertyAdjacentVertexViaEdges(Direction.IN, key);
        }
    }

    private static void addVertexPropertyAdjacentVertexViaEdges(final AerospikeConnection db,
                                                                final Direction direction,
                                                                final FireflyVertex vertex,
                                                                final FireflyVertexProperty<?> fireflyVertexProperty) {
        // We need to loop through attached vertices, getting their edge in the opposite direction (which points at
        // this vertex) and then add the property.
        final Iterator<Edge> edges = vertex.edges(direction);

        // Set is switched since this is running on adjacent vertex.
        final String set = direction.equals(Direction.IN) ? db.OUT_VP_SET : db.IN_VP_SET;
        edges.forEachRemaining(edge -> {
            // Need to get vp of edge in opposite direction (which points from the adjacent vertex to this vertex).
            final FireflyId vpId = (direction.equals(Direction.IN)) ? ((FireflyEdge) edge).outVertexId() : ((FireflyEdge) edge).inVertexId();

            // Get all vertex property related bins.
            final FireflyRecord record = FireflyRecord.read(db, set, vpId);
            if (record == null) {
                LOG.error("Could not find {} record for vertex {} when adding properties to adjacent lists of vertex {}.", set, vpId, vertex.id());
                throw new RuntimeException(String.format("Could not find %s record for vertex %s when adding properties to adjacent lists of vertex %s.", set, vpId, vertex.id()));

            }

            final Map<String, List<Map<String, Object>>> vertexVPValue = (Map<String, List<Map<String, Object>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE);
            final Map<String, List<Map<String, Long>>> vertexVPTypeHint = (Map<String, List<Map<String, Long>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
            final Map<String, List<Map<String, Object>>> vertexVPId = (Map<String, List<Map<String, Object>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);

            if (vertexVPValue == null || !vertexVPValue.containsKey(edge.label()) ||
                    vertexVPTypeHint == null || !vertexVPTypeHint.containsKey(edge.label()) ||
                    vertexVPId == null || !vertexVPId.containsKey(edge.label())) {
                LOG.error("Vertex property list {} of vertex {} were missing edge label {} when adding properties to adjacent lists of vertex {}.", set, vpId, edge.label(), vertex.id());
                throw new RuntimeException(String.format("Vertex property list %s of vertex %s were missing edge label %s when adding properties to adjacent lists of vertex %s.", set, vpId, edge.label(), vertex.id()));
            }

            // To get our entry, we need to find which position this edge exists in in the master list of the vertex.
            final Map<String, List<Object>> edgeLabelToId = getOrDefaultHashMap(db, db.VERTEX_AERO_SET, vpId, direction.equals(Direction.IN) ? db.OUT_EDGES : db.IN_EDGES);
            if (!edgeLabelToId.containsKey(edge.label())) {
                LOG.error("Could not find edge label {} in vertex {} when adding properties to adjacent lists of vertex {}.", edge.label(), vertex.id(), vpId);
                throw new RuntimeException(String.format("Could not find edge label %s in vertex %s when adding properties to adjacent lists of vertex %s.", edge.label(), vertex.id(), vpId));
            }

            final List<Object> edgeIds = edgeLabelToId.get(edge.label());
            final List<Map<String, Object>> vertexPropertyValues = vertexVPValue.get(edge.label());
            final List<Map<String, Long>> vertexPropertyTypeHints = vertexVPTypeHint.get(edge.label());
            final List<Map<String, Object>> vertexPropertyIds = vertexVPId.get(edge.label());
            if (edgeIds.size() != vertexPropertyValues.size()) {
                LOG.error("Edge list size {} did not match vertex property list size {} for vertex {} when adding properties to adjacent lists of vertex {}.", edgeIds.size(), vertexPropertyValues.size(), vpId, vertex.id());
                throw new RuntimeException(String.format("Edge list size %s did not match vertex property list size %s for vertex %s when adding properties to adjacent lists of vertex %s.", edgeIds.size(), vertexPropertyValues.size(), vpId, vertex.id()));
            }
            final int edgeIndex = findInList(edgeIds, ((FireflyEdge) edge).id.getStorageId(),
                                             String.format("Failed to find edge %s in vertex %s when adding properties to adjacent lists of vertex %s.", edge, vpId, vertex.id()));
            vertexPropertyValues.get(edgeIndex).put(fireflyVertexProperty.key(), fireflyVertexProperty.value());
            vertexVPValue.put(edge.label(), vertexPropertyValues);
            vertexPropertyTypeHints.get(edgeIndex).put(fireflyVertexProperty.key(), db.getSupportedType(fireflyVertexProperty.value().getClass()));
            vertexVPTypeHint.put(edge.label(), vertexPropertyTypeHints);
            vertexPropertyIds.get(edgeIndex).put(fireflyVertexProperty.key(), fireflyVertexProperty.id.getStorageId());
            vertexVPId.put(edge.label(), vertexPropertyIds);

            // Create bins for the maps.
            final Bin vertexPropertyIdMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexVPId));
            final Bin vertexPropertyValueMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(vertexVPValue));
            final Bin vertexPropertyTypeHintMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, Value.get(vertexVPTypeHint));

            // Write element. This isn't really an element, but the logic holds.
            FireflyRecord.writeElement(db, set, vpId, -1, vertexPropertyIdMapBin, vertexPropertyValueMapBin, vertexPropertyTypeHintMapBin);
        });
    }

    private void removeVertexPropertyAdjacentVertexViaEdges(final Direction direction, final String key) {
        final AerospikeConnection db = this.db;
        // We need to loop through attached vertices, getting their edge in the opposite direction (which points at
        // this vertex) and then add the property.
        final Iterator<Edge> edges = this.edges(direction);

        // Set is switched since this is running on adjacent vertex.
        final String set = direction.equals(Direction.IN) ? db.OUT_VP_SET : db.IN_VP_SET;
        edges.forEachRemaining(edge -> {
            // Need to get vp of edge in opposite direction (which points from the adjacent vertex to this vertex).
            final FireflyId vpId = (direction.equals(Direction.IN)) ? ((FireflyEdge) edge).outVertexId() : ((FireflyEdge) edge).inVertexId();

            // Get all vertex property related bins.
            final FireflyRecord record = FireflyRecord.read(db, set, vpId);
            if (record == null) {
                LOG.error("Could not find {} record for vertex {} when adding properties to adjacent lists of vertex {}.", set, vpId, this.id());
                throw new RuntimeException(String.format("Could not find %s record for vertex %s when adding properties to adjacent lists of vertex %s.", set, vpId, this.id()));

            }

            final Map<String, List<Map<String, Object>>> vertexVPValue = (Map<String, List<Map<String, Object>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE);
            final Map<String, List<Map<String, Long>>> vertexVPTypeHint = (Map<String, List<Map<String, Long>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
            final Map<String, List<Map<String, Object>>> vertexVPId = (Map<String, List<Map<String, Object>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);

            if (vertexVPValue == null || !vertexVPValue.containsKey(edge.label()) ||
                    vertexVPTypeHint == null || !vertexVPTypeHint.containsKey(edge.label()) ||
                    vertexVPId == null || !vertexVPId.containsKey(edge.label())) {
                LOG.error("Vertex property list {} of vertex {} were missing edge label {} when adding properties to adjacent lists of vertex {}.", set, vpId, edge.label(), this.id());
                throw new RuntimeException(String.format("Vertex property list %s of vertex %s were missing edge label %s when adding properties to adjacent lists of vertex %s.", set, vpId, edge.label(), this.id()));
            }

            // To get our entry, we need to find which position this edge exists in in the master list of the vertex.
            final Map<String, List<Object>> edgeLabelToId = getOrDefaultHashMap(db, db.VERTEX_AERO_SET, vpId, direction.equals(Direction.IN) ? db.OUT_EDGES : db.IN_EDGES);
            if (!edgeLabelToId.containsKey(edge.label())) {
                LOG.error("Could not find edge label {} in vertex {} when adding properties to adjacent lists of vertex {}.", edge.label(), this.id(), vpId);
                throw new RuntimeException(String.format("Could not find edge label %s in vertex %s when adding properties to adjacent lists of vertex %s.", edge.label(), this.id(), vpId));
            }

            final List<Object> edgeIds = edgeLabelToId.get(edge.label());
            final List<Map<String, Object>> vertexPropertyValues = vertexVPValue.get(edge.label());
            final List<Map<String, Long>> vertexPropertyTypeHints = vertexVPTypeHint.get(edge.label());
            final List<Map<String, Object>> vertexPropertyIds = vertexVPId.get(edge.label());
            if (edgeIds.size() != vertexPropertyValues.size()) {
                LOG.error("Edge list size {} did not match vertex property list size {} for vertex {} when adding properties to adjacent lists of vertex {}.", edgeIds.size(), vertexPropertyValues.size(), vpId, this.id());
                throw new RuntimeException(String.format("Edge list size %s did not match vertex property list size %s for vertex %s when adding properties to adjacent lists of vertex %s.", edgeIds.size(), vertexPropertyValues.size(), vpId, this.id()));
            }
            final int edgeIndex = findInList(edgeIds, ((FireflyEdge) edge).id.getStorageId(),
                                             String.format("Failed to find edge %s in vertex %s when adding properties to adjacent lists of vertex %s.", edge, vpId, this.id()));
            vertexPropertyValues.get(edgeIndex).remove(key);
            vertexVPValue.put(edge.label(), vertexPropertyValues);
            vertexPropertyTypeHints.get(edgeIndex).remove(key);
            vertexVPTypeHint.put(edge.label(), vertexPropertyTypeHints);
            vertexPropertyIds.get(edgeIndex).remove(key);
            vertexVPId.put(edge.label(), vertexPropertyIds);

            // Create bins for the maps.
            final Bin vertexPropertyIdMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexVPId));
            final Bin vertexPropertyValueMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(vertexVPValue));
            final Bin vertexPropertyTypeHintMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, Value.get(vertexVPTypeHint));

            // Write element. This isn't really an element, but the logic holds.
            FireflyRecord.writeElement(db, set, vpId, -1, vertexPropertyIdMapBin, vertexPropertyValueMapBin, vertexPropertyTypeHintMapBin);
        });
    }

    // TODO: Use this in other places.
    private static <K, U> Map<K, U> getOrDefaultHashMap(final AerospikeConnection db,
                                                        final String set,
                                                        final FireflyId id,
                                                        final String bin) {
        final FireflyRecord record = FireflyRecord.read(db, set, id);
        if (record == null) {
            return new HashMap<>();
        }
        final Map<K, U> map = (Map<K, U>) record.record.getMap(bin);
        return (map == null) ? new HashMap<>() : map;
    }

    public static void removeAdjacentVertexPropertiesFromVertex(final AerospikeConnection db,
                                                                final FireflyEdge edge,
                                                                final Direction direction) {
        // Need the vertex property set name for the vertex to remove the properties from.
        // We also need the vertex id to remove from.
        // If we are going IN, we will be removing the in properties from the in vertex.
        // If we are going OUT, we will be removing the out properties from the out vertex.
        final String vertexVPSet = direction.equals(Direction.IN) ? db.IN_VP_SET : db.OUT_VP_SET;
        final FireflyId vertexId = (direction.equals(Direction.IN)) ? edge.inVertexId() : edge.outVertexId();

        // Get all vertex property related bins.
        final FireflyRecord record = FireflyRecord.read(db, vertexVPSet, vertexId);
        if (record == null) {
            // Indicates vertex is already removed, so we can just return.
            return;
        }

        //
        final Map<String, List<Map<String, ?>>> vertexVPValue = (Map<String, List<Map<String, ?>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE);
        final Map<String, List<Map<String, ?>>> vertexVPTypeHint = (Map<String, List<Map<String, ?>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
        final Map<String, List<Map<String, ?>>> vertexVPId = (Map<String, List<Map<String, ?>>>) record.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);

        // The tricky part is what entry of the list inside the map. Using the edge label we can get the list from the map,
        // but we still need to know the index to remove. To get that we must read the vertex and get the index from the
        // vertex record by reading the edge id from the edge label to id list map of the vertex.
        final FireflyRecord vertexRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, vertexId);
        if (vertexRecord == null) {
            // Indicates vertex is already removed, so we can exit. The StarVertex removal is responsible for removing
            // the actual record.
            return;
        }

        final String vertexEdgeMapBin = direction.equals(Direction.IN) ? db.IN_EDGES : db.OUT_EDGES;
        final Map<String, List<Object>> vertexEdgeLabelToId = (Map<String, List<Object>>) vertexRecord.record.getMap(vertexEdgeMapBin);
        final List<Object> edgeIds = vertexEdgeLabelToId.get(edge.label());
        final int edgeIndex = findInList(edgeIds, edge.id.getStorageId(),
                                         String.format("Failed to find edge %s in vertex %s when removing adjacent vertex properties.", edge, vertexId));

        // Remove the vertex property values.
        final String errorMessageFormat = "Failed to remove %s from vertex %s when removing adjacent vertex properties for edge %s.";
        removeFromMap(vertexVPId, edge.label(), edgeIndex, edgeIds.size() <= 1, errorMessageFormat, "vertex property ids", vertexId, edge);
        removeFromMap(vertexVPValue, edge.label(), edgeIndex, edgeIds.size() <= 1, errorMessageFormat, "vertex property values", vertexId, edge);
        removeFromMap(vertexVPTypeHint, edge.label(), edgeIndex, edgeIds.size() <= 1, errorMessageFormat, "vertex property type hints", vertexId, edge);

        // Create bins for the maps.
        final Bin vertexPropertyIdMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexVPId));
        final Bin vertexPropertyValueMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(vertexVPValue));
        final Bin vertexPropertyTypeHintMapBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, Value.get(vertexVPTypeHint));

        // Write element. This isn't really an element, but the logic holds.
        FireflyRecord.writeElement(db, vertexVPSet, vertexId, -1, vertexPropertyIdMapBin, vertexPropertyValueMapBin, vertexPropertyTypeHintMapBin);
    }


    public static void removeCompoundEdgesFromAdjacentVertices(final AerospikeConnection db,
                                                               final FireflyEdge compoundEdge,
                                                               final boolean enableOutOut,
                                                               final boolean enableOutIn,
                                                               final boolean enableInOut,
                                                               final boolean enableInIn) {
        // Need to get adjacent vertices to start.
        final FireflyVertex inVertex = (FireflyVertex) compoundEdge.inVertex();
        final FireflyVertex outVertex = (FireflyVertex) compoundEdge.outVertex();

        // Now we want to do the following:
        //  1. Get the in vertices from our inVertex, and find the compound edges from there that go out.in (through inVertex) and remove them.
        //  2. Get the out vertices from our inVertex, and find the compound edges from there that go in.in (through inVertex) and remove them.
        //  3. Get the in vertices from our outVertex, and find the compound edges from there that go out.out (through outVertex) and remove them.
        //  4. Get the out vertices from our outVertex, and find the compound edges from there that go in.out (through outVertex) and remove them.

        // Go through inVertex edges.
        if (enableOutIn) {
            inVertex.edges(Direction.IN).forEachRemaining(edge -> {
                // If the edge is `this` edge, skip it. It is removed elsewhere.
                if (!edge.id().equals(compoundEdge.id())) {
                    // This set is in regard to the adjacent vertex so flip initial IN to OUT.
                    removeCompoundEdge(db, edge, compoundEdge, inVertex, Direction.IN, db.OUT_IN_SET, db.OUT_EDGES);
                }
            });
        }

        if (enableInIn) {
            inVertex.edges(Direction.OUT).forEachRemaining(edge -> {
                // If the edge is `this` edge, skip it. It is removed elsewhere.
                if (!edge.id().equals(compoundEdge.id())) {
                    // This set is in regard to the adjacent vertex so flip initial OUT to IN.
                    removeCompoundEdge(db, edge, compoundEdge, inVertex, Direction.OUT, db.IN_IN_SET, db.IN_EDGES);
                }
            });
        }

        // Go through outVertex edges.
        if (enableOutOut) {
            outVertex.edges(Direction.IN).forEachRemaining(edge -> {
                // If the edge is `this` edge, skip it. It is removed elsewhere.
                if (!edge.id().equals(compoundEdge.id())) {
                    // This set is in regard to the adjacent vertex so flip initial IN to OUT.
                    removeCompoundEdge(db, edge, compoundEdge, outVertex, Direction.IN, db.OUT_OUT_SET, db.OUT_EDGES);
                }
            });
        }

        if (enableInOut) {
            outVertex.edges(Direction.OUT).forEachRemaining(edge -> {
                // If the edge is `this` edge, skip it. It is removed elsewhere.
                if (!edge.id().equals(compoundEdge.id())) {
                    // This set is in regard to the adjacent vertex so flip initial OUT to IN.
                    removeCompoundEdge(db, edge, compoundEdge, outVertex, Direction.OUT, db.IN_OUT_SET, db.IN_EDGES);
                }
            });
        }
    }

    private static void removeCompoundEdge(final AerospikeConnection db,
                                           final Edge edge,
                                           final Edge compoundEdge,
                                           final FireflyVertex vertex,
                                           final Direction adjacentDirection,
                                           final String adjacentDirDirSet,
                                           final String adjacentVertexDirBin) {
        final FireflyId adjacentVertexId = adjacentDirection.equals(Direction.IN) ? ((FireflyEdge) edge).outVertexId() : ((FireflyEdge) edge).inVertexId();
        LOG.trace("Removing compound edge {} between vertex {} and vertex {} that connects using edge {}.", edge.id(), vertex.id(), adjacentVertexId, compoundEdge.id());

        // We need to be careful here because order matters.
        final FireflyRecord adjacentVertexDirDirRecord = FireflyRecord.read(db, adjacentDirDirSet, adjacentVertexId);
        final FireflyRecord adjacentVertexRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, adjacentVertexId);

        // Get adjacent vertex records edge map.
        final Map<String, List<Object>> adjacentVertexEdges;
        if (adjacentVertexRecord == null) {
            // First entry, generate empty map.
            adjacentVertexEdges = new HashMap<>();
        } else {
            adjacentVertexEdges = (Map<String, List<Object>>) adjacentVertexRecord.record.getMap(adjacentVertexDirBin);

            // If the bin is null it must be initialized.
            if (adjacentVertexEdges == null) {
                // This should never happen.
                LOG.error("Bin not found in adjacent vertex {} {}", adjacentVertexId, adjacentVertexDirBin);
                throw new RuntimeException(String.format("Bin not found in adjacent vertex %s %s", adjacentVertexId, adjacentVertexDirBin));
            }
        }

        final String edgeLabel = edge.label();
        if (!adjacentVertexEdges.containsKey(edgeLabel)) {
            // This should never happen.
            LOG.error("Edge label {} not found in adjacent vertex {}", edgeLabel, adjacentVertexId);
            throw new RuntimeException(String.format("Edge label %s not found in adjacent vertex %s", edgeLabel, adjacentVertexId));
        }

        final List<Object> edgeIds = adjacentVertexEdges.get(edgeLabel);
        final int edgeIndex = findInList(edgeIds, ((FireflyEdge) edge).id.getStorageId(),
                                         String.format("Failed to find edge id %s in edge set %s of map %s", edge.id(), edgeIds, adjacentVertexEdges));

        // Get the adjacent edge map.
        Map<String, List<Map<String, List<Object>>>> adjacentEdges;
        if (adjacentVertexDirDirRecord == null) {
            // First entry, generate empty map.
            adjacentEdges = new HashMap<>();
        } else {
            adjacentEdges = (Map<String, List<Map<String, List<Object>>>>) adjacentVertexDirDirRecord.record.getMap(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN);

            // If the bin is null it must be initialized.
            if (adjacentEdges == null) {
                adjacentEdges = new HashMap<>();
            }
        }
        List<Map<String, List<Object>>> adjacentVertexEdgeLabelToEdgeIds = adjacentEdges.get(edge.label());
        if (adjacentVertexEdgeLabelToEdgeIds == null) {
            // This should never happen.
            adjacentVertexEdgeLabelToEdgeIds = new ArrayList<>();
        }

        if (adjacentVertexEdgeLabelToEdgeIds.size() != edgeIds.size()) {
            // This should never happen.
            LOG.error("Mismatch in adjacent vertex edge sets of edge {} and vertex {}: {} {}", edgeLabel, adjacentVertexId, adjacentVertexEdgeLabelToEdgeIds, edgeIds);
            throw new RuntimeException(String.format("Mismatch in adjacent vertex edge %s and vertex %s: %s %s", edgeLabel, adjacentVertexId, adjacentVertexEdgeLabelToEdgeIds, edgeIds));
        }
        final Map<String, List<Object>> compoundEdgeMap = adjacentVertexEdgeLabelToEdgeIds.get(edgeIndex);
        final List<Object> compoundEdgeList = compoundEdgeMap.get(compoundEdge.label());
        final int compoundIndex = findInList(compoundEdgeList, ((FireflyEdge) compoundEdge).id.getStorageId(),
                                             String.format("Failed to find compound edge id %s in compound edge set %s of map %s", compoundEdge.id(), compoundEdgeList, compoundEdgeMap));

        if (compoundEdgeList.size() == 1) {
            // Remove the compound edge from the compound edge map.
            compoundEdgeMap.remove(compoundEdge.label());
        } else {
            // Remove the compound edge from the compound edge list.
            compoundEdgeList.remove(compoundIndex);
        }

        final Bin bin = new Bin(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, Value.get(adjacentEdges));
        FireflyRecord.writeElement(db, adjacentDirDirSet, adjacentVertexId, -1, bin);
    }

    public static void removeCompoundEdgesFromVertex(final AerospikeConnection db,
                                                     final Edge edge,
                                                     final FireflyVertex vertex,
                                                     final Direction direction,
                                                     final Direction secondaryDirection) {
        // Need to deal with ordering here, unfortunately.
        final String dirSet;
        if (direction.equals(Direction.IN)) {
            dirSet = secondaryDirection.equals(Direction.IN) ? db.IN_IN_SET : db.IN_OUT_SET;
        } else {
            dirSet = secondaryDirection.equals(Direction.IN) ? db.OUT_IN_SET : db.OUT_OUT_SET;
        }

        // Get this vertices current edgeLabel->list of edgeLabel->edgeIds map.
        final Map<String, List<Map<String, List<Object>>>> vertexDirEdgeMap = getOrDefaultHashMap(db, dirSet, vertex.id, db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN);

        // This adjacent vertex edgeLabel->edgeIds map.
        final Map<String, List<Object>> vertexEdgeMap = getOrDefaultHashMap(db, db.VERTEX_AERO_SET, vertex.id, direction.equals(Direction.IN) ? db.IN_EDGES : db.OUT_EDGES);
        final List<Object> edges = vertexEdgeMap.getOrDefault(edge.label(), new ArrayList<>());
        final int listIndex = findInList(edges, ((FireflyEdge) edge).id.getStorageId(), "Failed to find edge %s in vertex %s", edge.id(), vertex.id);

        // Need to find index of list.
        final List<Map<String, List<Object>>> edgeMapList = vertexDirEdgeMap.getOrDefault(edge.label(), new ArrayList<>());

        if (edges.size() > 1) {
            edgeMapList.remove(listIndex);
            vertexDirEdgeMap.put(edge.label(), edgeMapList);
        } else {
            vertexDirEdgeMap.remove(edge.label());
        }

        // Create bins for the new maps.
        final Bin bin = new Bin(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, Value.get(vertexDirEdgeMap));

        // Insert in Aerospike.
        LOG.trace("Removing compound edges for vertex {} on set {}={}", vertex.id, dirSet, vertexDirEdgeMap);
        FireflyRecord.writeElement(db, dirSet, vertex.id, -1, bin);
    }

    public static void removeCompoundEdges(final AerospikeConnection db,
                                           final FireflyEdge edge,
                                           final boolean enableOutOut,
                                           final boolean enableOutIn,
                                           final boolean enableInOut,
                                           final boolean enableInIn) {
        FireflyVertex inVertex = (FireflyVertex) edge.inVertex();
        FireflyVertex outVertex = (FireflyVertex) edge.outVertex();
        if (enableOutOut) {
            removeCompoundEdgesFromVertex(db, edge, outVertex, Direction.OUT, Direction.OUT);
        }
        if (enableOutIn) {
            removeCompoundEdgesFromVertex(db, edge, outVertex, Direction.OUT, Direction.IN);
        }
        if (enableInOut) {
            removeCompoundEdgesFromVertex(db, edge, inVertex, Direction.IN, Direction.OUT);
        }
        if (enableInIn) {
            removeCompoundEdgesFromVertex(db, edge, inVertex, Direction.IN, Direction.IN);
        }
    }

    private static void removeFromMap(final Map<String, List<Map<String, ?>>> map,
                                      final String key,
                                      final int index,
                                      final boolean removeKey,
                                      final String errorMessageStrFmt,
                                      final Object... errorMessageArgs) {
        final List<?> list = map.get(key);
        if (list == null || list.size() <= index) {
            // This should never happen.
            final String errorMessage = String.format(errorMessageStrFmt, errorMessageArgs);
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        if (!removeKey)
            list.remove(index);
        else
            map.remove(key);
    }
}
