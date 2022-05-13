package com.aerospike.firefly.structure;

import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyEdgeIterator<T> extends FireflyElementIterator<Edge> {
    protected FireflyEdgeIterator(FireflyGraph graph, Iterator<T> idIterator) {
        super(graph.db, idIterator, id -> graph.db.readEdge(graph,id));
    }
}
