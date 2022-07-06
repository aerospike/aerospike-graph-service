package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.utils.BloomFilterIdCache;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.structure.id.FireflyId;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;

import static com.aerospike.firefly.io.AerospikeConnection.SupportedValueTypes;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.USER_SUPPLIED_ID_EDGE_CACHE;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.USER_SUPPLIED_ID_VERTEX_CACHE;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class FireflyHelper {
    private FireflyHelper() {
    }

    public static boolean inComputerMode(final FireflyGraph graph) {
        return false;
    }

    public static FireflyVertex readVertex(FireflyGraph graph, FireflyId id) {
        return graph.getBaseGraph().readVertex(graph, id);
    }

    public static void writeVertex(FireflyGraph graph, FireflyId id, String label) {
        graph.getBaseGraph().writeVertex(graph, id, label);
    }

    public static void removeVertex(FireflyGraph graph, FireflyId id) {
        graph.getBaseGraph().removeVertex(graph, id);
    }


    public static Edge addEdge(final FireflyGraph graph, final FireflyVertex outVertex, final FireflyVertex inVertex, final String label, final Object... keyValues) {
        FireflyId fid = FireflyId.createFromKeyValuesOrManager(graph, FireflyEdge.class, keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
            FireflyHelper.validateEdgeId(fid, graph.getBaseGraph());
        } else {
            while (graph.getBaseGraph().edgeExists(fid)) {
                fid = FireflyId.createFromManager(graph, FireflyEdge.class);
            }
        }
        graph.getBaseGraph().writeEdge(graph, fid, label, outVertex, inVertex, new Object[]{});
        FireflyEdge edge = new FireflyEdge(fid, label, outVertex.id, inVertex.id, graph);
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    public static void validateVertexId(final FireflyId idValue,
                                        final AerospikeConnection db) {
        validateId(USER_SUPPLIED_ID_VERTEX_CACHE,
                idValue,
                Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported(),
                Graph.Exceptions.vertexWithIdAlreadyExists(idValue.value()),
                db,
                (FireflyId) -> db.vertexExists(idValue));
    }

    public static void validateEdgeId(final FireflyId idValue,
                                      final AerospikeConnection db) {
        validateId(USER_SUPPLIED_ID_EDGE_CACHE,
                idValue,
                Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported(),
                Graph.Exceptions.edgeWithIdAlreadyExists(idValue.value()),
                db,
                (FireflyId) -> db.edgeExists(idValue));
    }

    public static void validateId(final String cache,
                                  final FireflyId idValue,
                                  final UnsupportedOperationException unsupportedOperationException,
                                  final IllegalArgumentException illegalArgumentException,
                                  final AerospikeConnection db,
                                  final ExistsFunction existsFunction) {
        // Check to see if the id is user supplied. If so we must validate that it is not already in use.
        // Get id as a long.
        long idLong;
        try {
            // Convert id to long and check bloom filter. If the id is not available throw vertex with id already exists exception.
            idLong = NumericIdManager.convert(idValue.value());
        } catch (IllegalArgumentException ignored) {
            // Invalid type for id.
            throw unsupportedOperationException;
        }

        // Check if id is inside bloom filter.
        if (!BloomFilterIdCache.takeIdIfAvailable(db.getClient(), db.namespace, cache, idLong)
                && existsFunction.exists(idValue)) {
            throw illegalArgumentException;
        }
    }



    public interface ExistsFunction {
        boolean exists(final FireflyId idValue);
    }

    public static <V> V validateGraphVariableValue(V v) {
        Set<Class<? extends Serializable>> supported = SupportedValueTypes.keySet();
        if (v != null && !supported.contains(v.getClass()))
            throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(v);
        return v;
    }

    public static <V> V validatePropertyValue(V v) {
        Set<Class<? extends Serializable>> supported = SupportedValueTypes.keySet();
        if (v != null && !supported.contains(v.getClass()))
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(v);
        return v;
    }

    public static void legalPropertyKeyValueArray(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Iterator i = IteratorUtils.asIterator(keyValues);
        while (i.hasNext()) {
            Object key = i.next();
            if (String.class.equals(key.getClass())) {
                if (key.toString().isEmpty())
                    throw Property.Exceptions.propertyKeyCanNotBeEmpty();
                else if (key == null)
                    throw Property.Exceptions.propertyKeyCanNotBeNull();
            }

            i.next();
        }
    }

    public static Iterator<Edge> getEdges(FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        AerospikeConnection db = ((FireflyGraph) vertex.graph()).getBaseGraph();
        final List<Edge> edges = new ArrayList<>();

        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).hasNext()) {
                if (edgeLabels.length == 0) {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).forEachRemaining(id -> Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(FireflyEdge.class, id))).ifPresent(edges::add));
                } else {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        Optional<Edge> e = Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(FireflyEdge.class, id)));
                        e.ifPresent(edge -> IteratorUtils.asIterator(edgeLabels).forEachRemaining(label -> {
                            if (label.equals(edge.label()))
                                edges.add(edge);
                        }));
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).hasNext()) {
                if (edgeLabels.length == 0) {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).forEachRemaining(id -> Optional.ofNullable(
                            db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(FireflyVertex.class, id))).ifPresent(edges::add));
                } else {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        Optional<Edge> e = Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(FireflyVertex.class, id)));
                        e.ifPresent(edge -> IteratorUtils.asIterator(edgeLabels).forEachRemaining(label -> {
                            if (label.equals(edge.label()))
                                edges.add(edge);
                        }));
                    });
                }
            }
        }
        return edges.iterator();
    }

    public static Iterator<Vertex> getVertices(FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        AerospikeConnection db = ((FireflyGraph) vertex.graph()).getBaseGraph();
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).hasNext()) {
                if (edgeLabels.length == 0) {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        vertices.add(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(FireflyVertex.class, id)).inVertex());
                    });
                } else {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        Edge e = db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(FireflyVertex.class, id));
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it.equals(e.label())))
                            vertices.add(e.inVertex());
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).hasNext()) {
                if (edgeLabels.length == 0) {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        vertices.add(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(FireflyVertex.class, id)).outVertex());
                    });
                } else {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        Edge e = db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(FireflyEdge.class, id));
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it.equals(e.label())))
                            vertices.add(e.outVertex());
                    });
                }
            }
        }
        return vertices.iterator();
    }

    public static Iterator<FireflyEdge> queryEdgeStringIndex(FireflyGraph graph, String key, Object value) {
        return graph.getBaseGraph().queryEdgePropertyStringMatchIndex(graph, key, value);
    }

    public static Iterator<? extends Edge> queryEdgeNumericIndex(FireflyGraph graph, String key, P<?> predicate) {
        if(predicate.getBiPredicate().equals(Compare.eq))
            return graph.getBaseGraph().queryEdgePropertyNumberMatchIndex(graph, key, predicate);
        else if (predicate.getBiPredicate().equals(Compare.lt))
            return graph.getBaseGraph().queryEdgePropertyNumberRangeIndex(graph, key, predicate);
        else if(predicate.getBiPredicate().equals(Compare.gt))
            return graph.getBaseGraph().queryEdgePropertyNumberRangeIndex(graph, key, predicate);
        else
            throw new RuntimeException("Predicate not supported on index query " + predicate.getBiPredicate());
    }
    public static Iterator<? extends Vertex> queryVertexByVertexPropertyStringIndex(FireflyGraph graph, String key, Object value) {
        return IteratorUtils.map(graph.getBaseGraph().queryVertexPropertyStringIndex(graph, key, value), vp -> vp.element());
    }

    public static Iterator<? extends Vertex> queryVertexByVertexPropertyNumericIndex(FireflyGraph graph, String key, P<?> predicate) {
        if(predicate.getBiPredicate().equals(Compare.eq))
            return IteratorUtils.map(graph.getBaseGraph().queryVertexPropertyNumberMatchIndex(graph, key, predicate), vp -> vp.element());
        else if (predicate.getBiPredicate().equals(Compare.lt))
            return IteratorUtils.map(graph.getBaseGraph().queryVertexPropertyNumberRangeIndex(graph, key, predicate), vp -> vp.element());
        else if(predicate.getBiPredicate().equals(Compare.gt))
            return IteratorUtils.map(graph.getBaseGraph().queryVertexPropertyNumberRangeIndex(graph, key, predicate), vp -> vp.element());
        else
            throw new RuntimeException("Predicate not supported on index query " + predicate.getBiPredicate());
    }

    public static long countVertices(FireflyGraph graph) {
        return graph.getBaseGraph().getVertexCount();
    }

    public static long countEdges(FireflyGraph graph) {
        return graph.getBaseGraph().getEdgeCount();
    }
}
