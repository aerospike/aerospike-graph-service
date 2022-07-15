package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyEdgeIterator<T> extends FireflyElementIterator<Edge> {
    public FireflyEdgeIterator(FireflyGraph graph, Iterator<T> idIterator) {
        super(graph.getBaseGraph(), idIterator,
                id -> graph.getBaseGraph().edgeBackend.edgeExists(FireflyId.of(FireflyEdge.class, id)),
                id -> graph.getBaseGraph().edgeBackend.readEdge(graph, FireflyId.of(FireflyEdge.class, id)));
    }
}
