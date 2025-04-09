package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

import static com.aerospike.firefly.olap.codec.RowCodec.BULK_COL;

public class Codec {
    private final RowCodec codec;
    private final Traversal traversal;
    private final Set<TraverserRequirement> requirements;
    private final TraversalMatrix traversalMatrix;

    public Codec(final Traversal traversal) {
        this.codec = RowCodecFactory.getInstance().getCodec(traversal.asAdmin().getTraverserRequirements());
        this.traversal = traversal;
        this.requirements = traversal.asAdmin().getTraverserRequirements();
        this.traversalMatrix = new TraversalMatrix(traversal.asAdmin());
    }

    public Row encode(final Traverser traverser) {
        return codec.encode(traverser);
    }

    public Row encode(final Vertex vertex, final String step) {
        return codec.encode(vertex, traversalMatrix, step);
    }

    public Row encode(final Edge edge, final String step) {
        return codec.encode(edge, traversalMatrix, step);
    }

    public Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm) {
        return codec.decode(row, tg, tm);
    }

    public StructType getSchema() {
        return codec.getSchema();
    }

    public int getBulkedOrdinal() {
        return codec.columnToOrdinal.get(BULK_COL);
    }

    public boolean isBulkingSupported() {
        return requirements.contains(TraverserRequirement.BULK);
    }
}
