package com.aerospike.firefly.olap.codec.encoder;

import com.aerospike.firefly.olap.codec.Codec;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.EnumSet;
import java.util.Set;

import static com.aerospike.firefly.olap.codec.Codec.getIdType;

public class B_O_RowEncoder implements RowEncoder {
    private static final B_O_RowEncoder INSTANCE = new B_O_RowEncoder();

    private B_O_RowEncoder() {
    }

    public static B_O_RowEncoder getInstance() {
        return INSTANCE;
    }

    @Override
    public Row encode(final Traverser traverser) {
        if (traverser.get() instanceof Vertex) {
            final Vertex vertex = (Vertex) traverser.get();
            return RowFactory.create(
                    Codec.TRAVERSER_TYPE.VERTEX.ordinal(), // Integer traverser type.
                    vertex.id().toString(), // String id.
                    getIdType(vertex.id()).ordinal(), // Integer ordinal.
                    vertex.label(), // String label.
                    traverser.asAdmin().isHalted(), // Boolean halted.
                    traverser.asAdmin().getStepId(), // String step.
                    traverser.bulk()); // Integer bulk.
        } else if (traverser.get() instanceof Edge) {
            final Edge edge = (Edge) traverser.get();
            return RowFactory.create(
                    Codec.TRAVERSER_TYPE.EDGE.ordinal(), // Integer traverser type.
                    edge.id().toString(), // String id.
                    getIdType(edge.id()).ordinal(), // Integer ordinal.
                    edge.label(), // String label.
                    traverser.asAdmin().isHalted(), // Boolean halted.
                    traverser.asAdmin().getStepId(), // String step.
                    traverser.bulk()); // Integer bulk.
        } else {
            throw new RuntimeException("Error, encoder for " + traverser.get().getClass() + " is not implemented");
        }
    }

    @Override
    public Row encode(final Vertex vertex, final String step) {
        return RowFactory.create(Codec.TRAVERSER_TYPE.VERTEX.ordinal(), vertex.id().toString(), getIdType(vertex.id()).ordinal(), vertex.label(), false, step, 1L);
    }

    @Override
    public Row encode(final Edge edge, final String step) {
        return RowFactory.create(Codec.TRAVERSER_TYPE.EDGE.ordinal(), edge.id().toString(), getIdType(edge.id()).ordinal(), edge.label(), false, step, 1L);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return B_O_TraverserGenerator.instance().getProvidedRequirements();
    }
}
