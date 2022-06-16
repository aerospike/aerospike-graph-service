package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyId;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.iterator.FireflyElementIterator;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertexIterator<T> extends FireflyElementIterator<Vertex> {
    protected FireflyVertexIterator(FireflyGraph graph, Iterator<T> idIterator) {
        super(graph.db, idIterator,
                id -> graph.getBaseGraph().vertexExists(FireflyId.of(graph.db, FireflyVertex.class, id)),
                id -> graph.getBaseGraph().readVertex(graph, FireflyId.of(graph.db,FireflyVertex.class, id)));
    }
}
