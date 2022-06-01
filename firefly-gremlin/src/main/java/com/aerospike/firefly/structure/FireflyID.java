package com.aerospike.firefly.structure;

import com.aerospike.client.Record;

import com.aerospike.client.Key;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyID {
    private final Class<? extends Serializable> userClass;
    private final Class<? extends Serializable> storageClass;
    private final Object value;

    private FireflyID(Class<? extends Serializable> userClass, Class<? extends Serializable> storageClass, Object value) {
        this.userClass = userClass;
        this.storageClass = storageClass;
        this.value = value;
    }

    public FireflyID fromUser(Object id) {
        return null;
    }

    public FireflyID fromManager(Class<? extends FireflyElement> type) {
        return null;
    }

    public FireflyID fromAerospike(Key key, Record record) {
        return null;
    }

    public Key getKey() {
        return null;
    }


}
