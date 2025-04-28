package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;

import java.util.Set;

import static com.aerospike.firefly.olap.codec.RowCodec.BULK_COL;

public class TraverserCodec implements Codec {
    private final RowCodec codec;
    private final Set<TraverserRequirement> requirements;
    private final TraversalMatrix traversalMatrix;
    private final TraverserGenerator traverserGenerator;

    public TraverserCodec(final Traversal traversal) {
        this.codec = RowCodecFactory.getInstance().getCodec(traversal.asAdmin().getTraverserRequirements());
        this.requirements = traversal.asAdmin().getTraverserRequirements();
        this.traversalMatrix = new TraversalMatrix(traversal.asAdmin());
        this.traverserGenerator = traversal.asAdmin().getTraverserGenerator();
    }

    @Override
    public Row encode(final Traverser traverser) {
        return codec.encode(traverser);
    }

    @Override
    public Traverser decode(final Row row) {
        return codec.decode(row, traverserGenerator, traversalMatrix);
    }

    @Override
    public StructType getSchema() {
        return codec.getSchema();
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return this.traverserGenerator;
    }

    public int getBulkedOrdinal() {
        return codec.columnToOrdinal.get(BULK_COL);
    }

    public boolean isBulkingSupported() {
        return requirements.contains(TraverserRequirement.BULK);
    }
}
