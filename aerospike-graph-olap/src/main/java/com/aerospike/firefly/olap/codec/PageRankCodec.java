package com.aerospike.firefly.olap.codec;

import com.aerospike.firefly.olap.structure.MutableDetachedVertexProperty;
import com.aerospike.firefly.structure.FireflyVertex;
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

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.firefly.olap.codec.RowCodec.HALTED_COL;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getId;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getIdType;
import static com.aerospike.firefly.olap.helper.ProgramHelper.getVertexIdCount;
import static com.aerospike.firefly.olap.process.PageRankProgram.IN_VERTICES;
import static com.aerospike.firefly.olap.process.PageRankProgram.OUT_VERTEX_COUNT;

public class PageRankCodec implements Codec {
    public static final String IN_VERTEX_ID_COL = "~in_vertex_eid";
    public static final String OUT_VERTEX_COUNT_COL = "~out_vertex_count";
    public static final String PAGERANK_COL = "~pagerank";

    private final String property;
    private final TraverserGenerator traverserGenerator;

    public PageRankCodec(final Traversal traversal, final String property) {
        this.traverserGenerator = traversal.asAdmin().getTraverserGenerator();
        this.property = property;
    }

    @Override
    public Row encode(final Traverser traverser) {
        final Vertex v = (Vertex) traverser.get();

        final List<Object> objects = new ArrayList<>();
        objects.add(v.id().toString()); // 0. String id.
        objects.add(getIdType(v.id()).ordinal()); // 1

        // TODO: Consider serializing this into 1 large buffer.
        final List<byte[]> data = getInVertexIds(v);
        final byte[] dataAll = new byte[data.size() * 20]; // 20 bytes per vertex id.
        for (int i = 0; i < data.size(); i++) {
            final byte[] vertexId = data.get(i);
            System.arraycopy(vertexId, 0, dataAll, i * 20, vertexId.length);
        }
        objects.add(dataAll); // 2
        objects.add(getOutVertexCount(v)); // 3
        final VertexProperty pagerankProperty = v.property(this.property);
        objects.add(pagerankProperty.isPresent() ? pagerankProperty.value() : -1.0); // 4

        // not halted for now
        objects.add(false); // 5

        return RowFactory.create(objects.toArray(new Object[0]));
    }

    @Override
    public Traverser decode(final Row row) {
        final Object id = getId(row.getString(0), row.getInt(1));

        final List<byte[]> inEdges = new ArrayList<>();
        final byte[] inEdgesCell = (byte[]) row.get(2);
        for (int i = 0; i < inEdgesCell.length; i += 20) {
            final byte[] vertexId = new byte[20];
            System.arraycopy(inEdgesCell, i, vertexId, 0, 20);
            inEdges.add(vertexId);
        }
        final long outVertexCount = row.get(3) == null ? 0 : row.getLong(3);
        final double pageRank = row.get(4) == null ? -1 : row.getDouble(4);

        final DetachedVertexProperty inVertexProperty = new DetachedVertexProperty(null, IN_VERTICES, inEdges, null);
        final DetachedVertexProperty outVertexCountProperty = new DetachedVertexProperty(null, OUT_VERTEX_COUNT, outVertexCount, null);
        final MutableDetachedVertexProperty pagerankProperty = new MutableDetachedVertexProperty(null, property, pageRank, null);

        final DetachedVertex vertex = new DetachedVertex(id, "", List.of(inVertexProperty, outVertexCountProperty, pagerankProperty));
        return traverserGenerator.generate(vertex, EmptyStep.instance(), 1);
    }

    @Override
    public StructType getSchema() {
        return new StructType()
                .add(ELEMENT_ID_COL, DataTypes.StringType, true)
                .add(ELEMENT_ID_TYPEHINT_COL, DataTypes.IntegerType, true)
                .add(IN_VERTEX_ID_COL, DataTypes.BinaryType, true)
                .add(OUT_VERTEX_COUNT_COL, DataTypes.LongType, true)
                .add(PAGERANK_COL, DataTypes.DoubleType, true)
                .add(HALTED_COL, DataTypes.BooleanType, false);
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return this.traverserGenerator;
    }

    public static List<byte[]> getInVertexIds(final Vertex vertex) {
        if (vertex instanceof DetachedVertex) {
            return (List<byte[]>) vertex.property(IN_VERTICES).value();
        }

        return ((FireflyVertex) vertex).getConvertedVertexIds(Direction.IN);
    }

    public static Long getOutVertexCount(final Vertex vertex) {
        return getVertexIdCount(vertex, OUT_VERTEX_COUNT, Direction.OUT);
    }
}
