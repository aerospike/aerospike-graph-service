package com.aerospike.firefly.olap.codec.encoder;

import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

public interface RowEncoder {
    Row encode(final Traverser traverser);
    Row encode(final Vertex vertex, final String step);
    Row encode(final Edge edge, final String step);
    Set<TraverserRequirement> getRequirements();
}
