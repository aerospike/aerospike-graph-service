package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

public class Codec {
    private final RowCodec codec;

    public Codec(final Set<TraverserRequirement> traverserRequirements) {
        this.codec = RowCodecFactory.getInstance().getCodec(traverserRequirements);
    }

    public Row encode(final Traverser traverser) {
        return codec.encode(traverser);
    }

    public Row encode(final Vertex vertex, final String step) {
        return codec.encode(vertex, step);
    }

    public Row encode(final Edge edge, final String step) {
        return codec.encode(edge, step);
    }

    public Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm) {
        return codec.decode(row, tg, tm);
    }

    public StructType getSchema() {
        return codec.getSchema();
    }
}
