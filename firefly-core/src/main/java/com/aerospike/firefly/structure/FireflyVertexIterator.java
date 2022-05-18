package com.aerospike.firefly.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertexIterator<T> extends FireflyElementIterator<Vertex> {
    protected FireflyVertexIterator(FireflyGraph graph, Iterator<T> idIterator) {
        super(graph.db, idIterator, id -> graph.db.readVertex(graph,id));
    }
}
