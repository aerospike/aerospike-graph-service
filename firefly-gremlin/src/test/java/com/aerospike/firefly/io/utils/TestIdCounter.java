package com.aerospike.firefly.io.utils;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestIdCounter {

    private Configuration configuration;
    private AerospikeConnection db;

    @Before
    public void setup() {
        configuration = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        db = AerospikeConnection.connect(configuration);
        db.dropDatabase();
    }

    @After
    public void cleanup() {
        db.dropDatabase();
        db.close();
    }

    @Test
    public void testCounterOps() {
        IdCounter.zeroIdCounter(db.GLOBAL, db);
        IdCounter.incrementAndGetIdCounter(db.GLOBAL, db.namespace, db.getClient());
        assertEquals(1, IdCounter.getIdCounter(db.GLOBAL, db.namespace, db.getClient()));
        IdCounter.decrementIdCounter(db.GLOBAL, db.namespace, db.getClient());
        assertEquals(0, IdCounter.getIdCounter(db.GLOBAL, db.namespace, db.getClient()));
        IdCounter.incrementAndGetIdCounter(db.GLOBAL, db.namespace, db.getClient());
        IdCounter.incrementAndGetIdCounter(db.GLOBAL, db.namespace, db.getClient());
        assertEquals(2, IdCounter.getIdCounter(db.GLOBAL, db.namespace, db.getClient()));

        long res = IdCounter.greaterOrIncrement(36, db.GLOBAL, db.namespace, db.getClient());
        assertEquals(IdCounter.getIdCounter(db.GLOBAL, db.namespace, db.getClient()), res);
        assertEquals(36, res);

        IdCounter.zeroIdCounter(db.GLOBAL, db);
        assertEquals(0, IdCounter.getIdCounter(db.GLOBAL, db.namespace, db.getClient()));
        IdCounter.incrementAndGetIdCounter(db.GLOBAL, db.namespace, db.getClient());
        IdCounter.incrementAndGetIdCounter(db.GLOBAL, db.namespace, db.getClient());
        IdCounter.incrementAndGetIdCounter(db.GLOBAL, db.namespace, db.getClient());
        IdCounter.incrementAndGetIdCounter(db.GLOBAL, db.namespace, db.getClient());
        assertEquals(4, IdCounter.getIdCounter(db.GLOBAL, db.namespace, db.getClient()));
        long res2 = IdCounter.greaterOrIncrement(3, db.GLOBAL, db.namespace, db.getClient());
        assertEquals(5, IdCounter.getIdCounter(db.GLOBAL, db.namespace, db.getClient()));
        assertEquals(5, res2);
        assertEquals(5, IdCounter.getIdCounter(db.GLOBAL, db.namespace, db.getClient()));
        assertEquals(5, IdCounter.greaterOrExisting(3,db.GLOBAL, db.namespace, db.getClient()));
        assertEquals(5, IdCounter.greaterOrExisting(3,db.GLOBAL, db.namespace, db.getClient()));
    }
}
