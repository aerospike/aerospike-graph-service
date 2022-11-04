package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertexIterator<T> extends FireflyElementIterator<Vertex> {
    public FireflyVertexIterator(FireflyGraph graph, Iterator<T> idIterator) {
        super(graph.getBaseGraph(), idIterator,
                id -> graph.vertexExists(FireflyIdFactory.createId(id)),
                id -> graph.readVertex(FireflyIdFactory.createId(id)));
    }
}
