package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyId;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.iterator.FireflyElementIterator;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertexPropertyIterator extends FireflyElementIterator<Property> {
    protected FireflyVertexPropertyIterator(FireflyVertex vertex, AerospikeConnection db, Iterator<?> idIterator) {
        super(db, idIterator,
                id -> db.vertexPropertyExists(FireflyId.of(((FireflyGraph) vertex.graph()).getBaseGraph(), FireflyVertexProperty.class, id)),
                id -> db.readVertexProperty(vertex, FireflyId.of(((FireflyGraph) vertex.graph()).db,FireflyVertexProperty.class, id)));
    }
}
