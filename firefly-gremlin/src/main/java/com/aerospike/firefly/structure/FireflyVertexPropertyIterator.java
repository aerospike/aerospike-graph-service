package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertexPropertyIterator extends FireflyElementIterator<Property> {
    protected FireflyVertexPropertyIterator(FireflyVertex vertex, AerospikeConnection db, Iterator<?> idIterator) {
        super(db, idIterator,
                id -> db.vertexPropertyExists(FireflyId.of(FireflyVertexProperty.class, id)),
                id -> db.readVertexProperty(vertex, FireflyId.of(FireflyVertexProperty.class, id)));
    }
}
