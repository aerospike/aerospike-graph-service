package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertexIterator extends FireflyElementIterator<Vertex> {
    public FireflyVertexIterator(FireflyGraph graph, Iterator<FireflyId> idIterator) {
        super(graph.getBaseGraph(),
                idIterator,
                id -> graph.vertexExists(graph.getIdFactory().createId(id, FireflyVertex.class)),
                id -> graph.readVertex(graph.getIdFactory().createId(id, FireflyEdge.class)));
    }
}
