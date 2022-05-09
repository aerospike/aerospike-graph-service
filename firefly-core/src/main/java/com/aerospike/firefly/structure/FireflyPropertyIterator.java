package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyPropertyIterator extends FireflyElementIterator<Property> {
    protected FireflyPropertyIterator(FireflyVertex vertex,AerospikeConnection db, Iterator<?> idIterator) {
        super(db, idIterator, o -> db.readProperty(vertex,o.toString()));
    }
}
