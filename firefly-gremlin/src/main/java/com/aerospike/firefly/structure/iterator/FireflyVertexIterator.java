package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertexIterator<T> extends FireflyElementIterator<Vertex> {
    public FireflyVertexIterator(FireflyGraph graph, Iterator<T> idIterator) {
        super(graph.getBaseGraph(), idIterator,
                id -> graph.vertexExists(FireflyId.of(FireflyVertex.class, id)),
                id -> graph.readVertex(FireflyId.of(FireflyVertex.class, id)));
    }
}
