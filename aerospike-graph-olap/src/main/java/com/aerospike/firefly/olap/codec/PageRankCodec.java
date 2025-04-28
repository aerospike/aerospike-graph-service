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
import static com.aerospike.firefly.olap.helper.ProgramHelper.getVertexIds;
import static com.aerospike.firefly.olap.process.PageRankProgram.OUT_VERTICES;
import static com.aerospike.firefly.olap.process.PageRankProgram.property;

public class PageRankCodec implements Codec {
    public static final String ELEMENT_ID_COL = "~eid";
    public static final String ELEMENT_ID_TYPEHINT_COL = "~eid_typehint";
    public static final String OUT_VERTEX_ID_COL = "~out_vertex_eid";
    public static final String PAGERANK_COL = "~pagerank";

    private final TraverserGenerator traverserGenerator;

    public PageRankCodec(final Traversal traversal) {
        this.traverserGenerator = traversal.asAdmin().getTraverserGenerator();
    }

    @Override
    public Row encode(final Traverser traverser) {
        final Vertex v = (Vertex) traverser.get();

        final List<Object> objects = new ArrayList<>();
        objects.add(v.id().toString()); // String id.
        objects.add(getIdType(v.id()).ordinal());

        objects.add(JavaConverters.asScalaBufferConverter(getOutVertexIds(v)).asScala().toSeq());
        final VertexProperty pagerankProperty = v.property(property);
        objects.add(pagerankProperty.isPresent() ? pagerankProperty.value() : -1.0);

        // not halted for now
        objects.add(false);

        return RowFactory.create(objects.toArray(new Object[0]));
    }

    @Override
    public Traverser decode(final Row row) {
        final Object id = getId(row.getString(0), row.getInt(1));

        final List<String> outEdges = new ArrayList<>();
        final List outRow = row.getList(2);
        outRow.forEach(r -> outEdges.add(r.toString()));

        final double pageRank = row.get(3) == null? -1 : row.getDouble(3);

        final DetachedVertexProperty outVertexProperty = new DetachedVertexProperty(null, OUT_VERTICES, outEdges, null);
        final MutableDetachedVertexProperty pagerankProperty = new MutableDetachedVertexProperty(null, property, pageRank, null);

        final DetachedVertex vertex = new DetachedVertex(id, "", List.of(outVertexProperty, pagerankProperty));
        return traverserGenerator.generate(vertex, EmptyStep.instance(), 1);
    }

    @Override
    public StructType getSchema() {
        return new StructType()
                .add(ELEMENT_ID_COL, DataTypes.StringType, true)
                .add(ELEMENT_ID_TYPEHINT_COL, DataTypes.IntegerType, true)
                .add(OUT_VERTEX_ID_COL, DataTypes.createArrayType(DataTypes.StringType), true)
                .add(PAGERANK_COL, DataTypes.DoubleType, true)
                .add(HALTED_COL, DataTypes.BooleanType, false);
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return this.traverserGenerator;
    }

    public static List<String> getOutVertexIds(final Vertex vertex) {
        return getVertexIds(vertex, OUT_VERTICES, Direction.OUT);
    }
}
