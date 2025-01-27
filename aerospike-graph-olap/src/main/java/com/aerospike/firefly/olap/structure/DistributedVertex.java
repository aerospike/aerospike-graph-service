package com.aerospike.firefly.olap.structure;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.iterator.FireflyBatchElementIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromIndexedVertex;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
// TODO: Should maybe make this detatchd however technically we have graph access in the contrusting class so attached is OK.
public class DistributedVertex extends DistributedElement implements Vertex {

    private final boolean isEdgeCacheOverflowed;
    private final scala.collection.immutable.Map<String, WrappedArray<byte[]>> inEdges;
    private final scala.collection.immutable.Map<String, WrappedArray<byte[]>> outEdges;
    private final scala.collection.immutable.Map<String, String> properties;
    private final FireflyGraph graph;
    private final AerospikeConnection db;
    private final Map<String, TraverserSet<Object>> computerComputeMap = new HashMap<>();
    // final FireflyId fid,
    //                         final String label,
    //                         final FireflyGraph graph,
    //                         final Map<String, List<LazyIdTransform>> inEdgeIds,
    //                         final Map<String, List<LazyIdTransform>> outEdgeIds,
    //                         final Map<String, LazyIdTransform> vertexPropertyIds,
    //                         final Map<String, Object> vertexPropertyValues,
    //                         final Map<String, Object> vertexPropertyValuesTypeHints,
    //                         final Map<Object, Map<String, Object>> vertexPropertyIdToProperties,
    //                         final Map<Object, Map<String, Object>> vertexPropertyIdToTypeHints,
    //                         final boolean isEdgeCacheOverflowed,
    //                         final AerospikeConnection db


    public DistributedVertex(final Row row, final FireflyGraph graph) {
        // Minor optimization would be to use scala maps directly here.
        super((String) row.get(row.fieldIndex(ID_COL)),
                (Integer) row.get(row.fieldIndex(ID_TYPEHINT_COL)),
                (String) row.get(row.fieldIndex(LABEL_COL)));
        this.properties = (scala.collection.immutable.Map) row.get(row.fieldIndex(PROPERTIES_COL));
        this.inEdges = (scala.collection.immutable.Map) row.get(row.fieldIndex(IN_COL));
        this.outEdges = (scala.collection.immutable.Map) row.get(row.fieldIndex(OUT_COL));
        this.graph = graph;
        this.db = graph.getBaseGraph();
        this.isEdgeCacheOverflowed = false; // TODO.
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {

        return null;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        final Set<String> edgeLabelSet = new HashSet<>(Arrays.asList(edgeLabels));
        final Iterator<FireflyId> adjacentVertices = getVertexIdsFromVertex(direction, edgeLabelSet);
        return new FireflyBatchElementIterator<>(this.graph, adjacentVertices, Collections.emptyList(), this.graph::readVertices, null);
    }

    public Iterator<FireflyId> getVertexIdsFromVertex(final Direction direction, final Set<String> labels) {
        final List<FireflyId> cachedIds = getCachedVertexIds(direction, labels);

        if (isEdgeCacheOverflowed) {
            return FireflyCloseableIteratorUtils.concat(cachedIds.iterator(), getSupernodeVertexIds(direction, labels));
        } else {
            return cachedIds.iterator();
        }
    }

    /**
     * Important note about this function: It only returns ids that are cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getCachedVertexIds.
     */
    private List<FireflyId> getCachedVertexIds(final Direction direction, final Set<String> labels) {
        return getCachedIds(direction, labels).stream().map(id -> ((FireflyIdComposite) id).getAdjacentId()).collect(Collectors.toList());
    }


    public List<FireflyId> getCachedIds(final Direction direction, final Set<String> labels) {
        // Get cached IDs
        final List<FireflyId> cachedIds = new ArrayList<>();
        if (direction == Direction.OUT || direction == Direction.BOTH) {
            final Map<String, List<byte[]>> outEdgeIds = getJavaEdges(Direction.OUT);
            for (final String key : outEdgeIds.keySet()) {
                if (labels.isEmpty() || labels.contains(key)) {
                    cachedIds.addAll(outEdgeIds.get(key).stream().map(f -> graph.getIdFactory().createCompositeEdgeId(f)).
                            collect(Collectors.toList()));
                }
            }
        }
        if (direction == Direction.IN || direction == Direction.BOTH) {
            final Map<String, List<byte[]>> inEdgeIds = getJavaEdges(Direction.IN);
            for (final String key : inEdgeIds.keySet()) {
                if (labels.isEmpty() || labels.contains(key)) {
                    cachedIds.addAll(inEdgeIds.get(key).stream().map(f -> graph.getIdFactory().createCompositeEdgeId(f)).
                            collect(Collectors.toList()));
                }
            }
        }
        return cachedIds;
    }


    /**
     * Important note about this function: It only returns ids that are NOT cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getSupernodeVertexIds.
     */
    public Iterator<FireflyId> getSupernodeVertexIds(final Direction direction, final Set<String> labels) {
        return getSupernodeIds(direction, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.VERTEX_ID, Collections.emptyList());
    }

    public Iterator<FireflyId> getSupernodeIds(final Direction direction,
                                               final Set<String> labels,
                                               final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                               final List<HasContainer> hasContainers) {
        if (!this.isEdgeCacheOverflowed) {
            return Collections.emptyIterator();
        }

        return getIdsFromVertexByIndex(direction, labels, outputType, hasContainers);
    }

    protected Iterator<FireflyId> getIdsFromVertexByIndex(final Direction direction,
                                                          final Set<String> labels,
                                                          final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                                          final List<HasContainer> hasContainers) {
        // Might be a screw up here somehow with these ids but i dont wanna think about it rn.
        final FireflyId ffid = graph.getIdFactory().createVertexId(id);
        if (direction == Direction.BOTH) {
            final Iterator<KeyRecord> inKeyRecordIterator = getEdgeKeyRecordsByIndex(Direction.IN, labels, outputType, hasContainers);
            final Iterator<KeyRecord> outKeyRecordIterator = getEdgeKeyRecordsByIndex(Direction.OUT, labels, outputType, hasContainers);
            return FireflyCloseableIteratorUtils.concat(new FireflyPhatEdgeIdIteratorFromIndexedVertex(inKeyRecordIterator, this.db, Direction.IN, ffid, labels, outputType, null),
                    new FireflyPhatEdgeIdIteratorFromIndexedVertex(outKeyRecordIterator, this.db, Direction.OUT, ffid, labels, outputType, null));
        } else {
            final Iterator<KeyRecord> keyRecordIterator = getEdgeKeyRecordsByIndex(direction, labels, outputType, hasContainers);
            return new FireflyPhatEdgeIdIteratorFromIndexedVertex(keyRecordIterator, this.db, direction, ffid, labels, outputType, null);
        }
    }

    /**
     * Get the KeyRecord iterator for Edges attached to this Vertex. Public only for testing purposes.
     *
     * @param direction
     * @param labels
     * @param outputType
     * @param hasContainers
     * @return KeyRecord iterator for Edges attached to this Vertex.
     */
    public Iterator<KeyRecord> getEdgeKeyRecordsByIndex(final Direction direction,
                                                        final Set<String> labels,
                                                        final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                                        final List<HasContainer> hasContainers) {// Might be a screw up here somehow with these ids but i dont wanna think about it rn.
        final FireflyId ffid = graph.getIdFactory().createVertexId(id);
        return graph.operations.getEdgeKeyRecordsByIndex(ffid, direction, labels, outputType, hasContainers, null);
    }

    public Map<String, String> getJavaProperties() {
        //final Map<String, String> javaMap
        final scala.collection.Iterator<Tuple2<String, String>> itty = properties.iterator();
        final Map<String, String> javaMap = new HashMap<>();
        while (itty.hasNext()) {
            final Tuple2<String, String> tuple = itty.next();
            javaMap.put(tuple._1, tuple._2);
        }
        return javaMap;


        //return JavaConverters.mapAsJavaMap(properties);
    }

    public scala.collection.immutable.Map<String, String> getScalaProperties() {
        return properties;
    }

    public Map<String, List<byte[]>> getJavaEdges(Direction direction) {
        if (direction.equals(Direction.IN)) {
            final Map<String, List<byte[]>> inEdgesConverted = new HashMap<>();
            final Map<String, WrappedArray<byte[]>> partialConverted = JavaConverters.mapAsJavaMap(inEdges);
            for (final String key : partialConverted.keySet()) {
                inEdgesConverted.put(key, JavaConverters.seqAsJavaListConverter(partialConverted.get(key)).asJava());
            }
            return inEdgesConverted;
        } else if (direction.equals(Direction.OUT)) {
            final Map<String, List<byte[]>> outEdgesConverted = new HashMap<>();
            final Map<String, WrappedArray<byte[]>> partialConverted = JavaConverters.mapAsJavaMap(outEdges);
            for (final String key : partialConverted.keySet()) {
                outEdgesConverted.put(key, JavaConverters.seqAsJavaListConverter(partialConverted.get(key)).asJava());
            }
            return outEdgesConverted;
        } else {
            // todo.
            throw new IllegalArgumentException("Both not implemented for getJavaEdges() in DistributedVertex");
        }
    }

    public scala.collection.immutable.Map<String, WrappedArray<byte[]>> getScalaEdges(Direction direction) {
        if (direction.equals(Direction.IN)) {
            return inEdges;
        } else if (direction.equals(Direction.OUT)) {
            return outEdges;
        } else {
            // todo.
            throw new IllegalArgumentException("Both not implemented for getScalaEdges() in DistributedVertex");
        }
    }



    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        final List<VertexProperty<V>> properties = new ArrayList<>();
        if (propertyKeys.length == 0) {
            final scala.collection.Iterator<Tuple2<String, String>> itty = this.properties.iterator();
            while (itty.hasNext()) {
                final Tuple2<String, String> keyValue = itty.next();
                properties.add(new DistributedVertexProperty<>(keyValue._1, keyValue._2));
            }
            for (final String key : computerComputeMap.keySet()) {
                properties.add(new DistributedVertexProperty<>(key, computerComputeMap.get(key)));
            }
        } else {
            for (int i = 0; i < propertyKeys.length; i++) {
                final String key = propertyKeys[i];

                if (this.properties.contains(key)) {
                    properties.add(new DistributedVertexProperty<>(key, this.properties.get(key).get()));
                } else if (this.computerComputeMap.containsKey(key)) {
                    properties.add(new DistributedVertexProperty<>(key, this.computerComputeMap.get(key)));
                }
            }
            System.out.println("???");
        }
        return properties.iterator();
        //if (propertyKeys.length == 0) {
        //    return (Iterator) properties.entrySet().stream().map(e -> new DistributedVertexProperty<>(e.getKey(), e.getValue())).iterator();
        //} else {
        //    return (Iterator) properties.entrySet().stream().filter(e -> {
        //        for (String key : propertyKeys) {
        //            if (e.getKey().equals(key)) {
        //                return true;
        //            }
        //        }
        //        return false;
        //    }).map(e -> new DistributedVertexProperty<>(e.getKey(), e.getValue())).iterator();
        //}
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.properties.contains(key)) {
            return new DistributedVertexProperty<>(key, this.properties.get(key));
        } else {
            return VertexProperty.empty();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (DistributedExecutor.getComputeKeyStrings().contains(key)) {
            if (computerComputeMap.containsKey(key)) {
                return new DistributedVertexProperty<>(key, computerComputeMap.get(key));
            } else {
                return VertexProperty.empty();
            }
        } else {
            if (properties.contains(key)) {
                return new DistributedVertexProperty<>(key, properties.get(key));
            } else {
                return VertexProperty.empty();
            }
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
