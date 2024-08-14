package com.aerospike.firefly.process.traversal.step.computer;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface PrecomputableComputerStep {
    void precompute();
    void add(Traverser.Admin<?> t, Vertex v);
    void release();
}
