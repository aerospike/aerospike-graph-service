package com.aerospike.firefly.olap.structure;

import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import scala.collection.JavaConverters;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class DistributedVertex extends DistributedElement implements Vertex {
    //private final Map<String, List<byte[]>> inEdges;
    //private final Map<String, List<byte[]>> outEdges;
    //private final Map<String, Object> properties;


    public DistributedVertex(final Row row) {
        // Minor optimization would be to use scala maps directly here.
        super(row.get(row.fieldIndex(ID_STRING)), (String) row.get(row.fieldIndex(LABEL_STRING)));
        //this.properties = JavaConverters.mapAsJavaMap((scala.collection.immutable.Map) row.get(row.fieldIndex(PROPERTIES_STRING)));
        //this.inEdges = JavaConverters.mapAsJavaMap((scala.collection.immutable.Map) row.get(row.fieldIndex(IN_STRING)));
        //this.outEdges = JavaConverters.mapAsJavaMap((scala.collection.immutable.Map) row.get(row.fieldIndex(OUT_STRING)));
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {

        return null;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return null;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
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
        return null;
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
       return null;// return new DistributedVertexProperty<>(key, (V) properties.get(key));
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
