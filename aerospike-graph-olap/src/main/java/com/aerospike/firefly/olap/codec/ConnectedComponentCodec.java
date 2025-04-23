package com.aerospike.firefly.olap.codec;

import com.aerospike.firefly.olap.structure.MutableDetachedVertexProperty;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.aerospike.firefly.olap.codec.RowCodec.HALTED_COL;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getId;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getIdType;
import static com.aerospike.firefly.olap.process.ConnectedComponentProgram.CONNECTED_VERTICES;
import static com.aerospike.firefly.olap.process.ConnectedComponentProgram.property;

public class ConnectedComponentCodec implements Codec {
    public static final String ELEMENT_ID_COL = "~eid";
    public static final String ELEMENT_ID_TYPEHINT_COL = "~eid_typehint";
    public static final String CONNECTED_VERTEX_ID_COL = "~connected_vertex_eid";
    public static final String COMPONENT_COL = "~component";

    private final TraverserGenerator traverserGenerator;

    public ConnectedComponentCodec(final Traversal traversal) {
        this.traverserGenerator = traversal.asAdmin().getTraverserGenerator();
    }

    @Override
    public Row encode(final Traverser traverser) {
        final Vertex v = (Vertex) traverser.get();

        final List<Object> objects = new ArrayList<>();
        objects.add(v.id().toString()); // String id.
        objects.add(getIdType(v.id()).ordinal());

        objects.add(JavaConverters.asScalaBufferConverter(connectedVertexIds(v)).asScala().toSeq());
        final VertexProperty component = v.property(property);
        objects.add(component.isPresent() ? component.value() : v.id().toString()); // starting component

        // not halted for now
        objects.add(false);

        return RowFactory.create(objects.toArray(new Object[0]));
    }

    @Override
    public Traverser decode(final Row row) {
        final Object id = getId(row.getString(0), row.getInt(1));

        final List<String> edges = new ArrayList<>();
        final List outRow = row.getList(2);
        outRow.forEach(r -> edges.add(r.toString()));

        final String component = row.get(3) == null ? id.toString() : row.getString(3);

        final DetachedVertexProperty connectedVertexProperty = new DetachedVertexProperty(null, CONNECTED_VERTICES, edges, null);
        final MutableDetachedVertexProperty componentProperty = new MutableDetachedVertexProperty(null, property, component, null);

        final DetachedVertex vertex = new DetachedVertex(id, "", List.of(connectedVertexProperty, componentProperty));
        return traverserGenerator.generate(vertex, EmptyStep.instance(), 1);
    }

    @Override
    public StructType getSchema() {
        return new StructType()
                .add(ELEMENT_ID_COL, DataTypes.StringType, true)
                .add(ELEMENT_ID_TYPEHINT_COL, DataTypes.IntegerType, true)
                .add(CONNECTED_VERTEX_ID_COL, DataTypes.createArrayType(DataTypes.StringType), true)
                .add(COMPONENT_COL, DataTypes.StringType, true)
                .add(HALTED_COL, DataTypes.BooleanType, false);
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return this.traverserGenerator;
    }

    public static List<String> connectedVertexIds(final Vertex vertex) {
        if (vertex instanceof DetachedVertex) {
            return (List<String>) vertex.property(CONNECTED_VERTICES).value();
        }
        final List<FireflyId> cachedIds = IteratorUtils.asList(
                ((FireflyVertex) vertex).getVertexIdsFromVertex(Direction.BOTH, Collections.emptySet()));

        final List<String> connectedVertexIds = new ArrayList<>(cachedIds.size());
        cachedIds.forEach(id -> connectedVertexIds.add(id.toString()));
        return connectedVertexIds;
    }
}
