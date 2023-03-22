package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyEdgeIterator extends FireflyElementIterator<Edge> {
    public FireflyEdgeIterator(final FireflyGraph graph, final Iterator<FireflyId> idIterator) {
        super(graph.getBaseGraph(), idIterator, graph::readEdge);
    }
}
