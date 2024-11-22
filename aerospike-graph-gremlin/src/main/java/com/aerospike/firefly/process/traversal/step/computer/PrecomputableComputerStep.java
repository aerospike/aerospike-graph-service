package com.aerospike.firefly.process.traversal.step.computer;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

public interface PrecomputableComputerStep<E extends Element> {
    void precompute();
    void add(Traverser.Admin<?> t, Vertex v);
    List<E> get();
    void release();
}
