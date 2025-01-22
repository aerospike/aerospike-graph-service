package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
// TODO: Should maybe make this detatchd however technically we have graph access in the contrusting class so attached is OK.
public class DistributedVertex extends DistributedElement implements Vertex {

    private final scala.collection.immutable.Map<String, List<byte[]>> inEdges;
    private final scala.collection.immutable.Map<String, List<byte[]>> outEdges;
    private final scala.collection.immutable.Map<String, String> properties;
    private final FireflyGraph graph;
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
        super((String) row.get(row.fieldIndex(ID_STRING)),
                (Integer) row.get(row.fieldIndex(ID_TYPEHINT_STRING)),
                (String) row.get(row.fieldIndex(LABEL_STRING)));
        this.properties = (scala.collection.immutable.Map) row.get(row.fieldIndex(PROPERTIES_STRING));
        this.inEdges = (scala.collection.immutable.Map) row.get(row.fieldIndex(IN_STRING));
        this.outEdges = (scala.collection.immutable.Map) row.get(row.fieldIndex(OUT_STRING));
        this.graph = graph;
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {

        return null;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return null;
    }

    public Map<String, String> getJavaProperties() {
        return JavaConverters.mapAsJavaMap(properties);
    }

    public scala.collection.immutable.Map<String, String> getScalaProperties() {
        return properties;
    }

    public Map<String, List<byte[]>> getJavaEdges(Direction direction) {
        if (direction.equals(Direction.IN)) {
            return JavaConverters.mapAsJavaMap(inEdges);
        } else if (direction.equals(Direction.OUT)) {
            return JavaConverters.mapAsJavaMap(outEdges);
        } else {
            // todo.
            throw new IllegalArgumentException("Both not implemented for getJavaEdges() in DistributedVertex");
        }
    }

    public scala.collection.immutable.Map<String, List<byte[]>> getScalaEdges(Direction direction) {
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
                    properties.add(new DistributedVertexProperty<>(key, this.properties.get(key)));
                } else if (this.computerComputeMap.containsKey(key)) {
                    properties.add(new DistributedVertexProperty<>(key, this.computerComputeMap.get(key)));
                }
            }
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
