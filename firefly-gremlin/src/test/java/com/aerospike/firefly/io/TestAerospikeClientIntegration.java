package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.Util;
import org.apache.commons.configuration2.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.io.AerospikeConnection.*;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeClientIntegration {

    private Configuration c;
    private AerospikeConnection db;

    @BeforeEach
    void setup() {
        c = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        db = AerospikeConnection.connect(ConfigurationHelper.aerospikeHost(c),
                ConfigurationHelper.aerospikePort(c),
                ConfigurationHelper.aerospikeNamespace(c));
    }

    @AfterEach
    void cleanup() {
        db.dropDatabase();
        db.close();
    }

    @Test
    void testConnectToAerospike() {
        Configuration c = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection ac = AerospikeConnection.connect(
                ConfigurationHelper.aerospikeHost(c),
                ConfigurationHelper.aerospikePort(c),
                ConfigurationHelper.aerospikeNamespace(c));
    }

    @Test
    void testBasicReadWrite() {
        Configuration c = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection ac = AerospikeConnection.connect(ConfigurationHelper.aerospikeHost(c), ConfigurationHelper.aerospikePort(c), ConfigurationHelper.aerospikeNamespace(c));
        Key key = new Key(ConfigurationHelper.aerospikeNamespace(c), TEST_SET, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        ac.write(key, bin1, bin2, bin3);
        assertEquals(ac.read(key).getInt("age"), 32);
    }

    @Test
    void testBasicDelete() {
        Configuration c = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection ac = AerospikeConnection.connect(ConfigurationHelper.aerospikeHost(c), ConfigurationHelper.aerospikePort(c), ConfigurationHelper.aerospikeNamespace(c));
        Key key = new Key(ConfigurationHelper.aerospikeNamespace(c), TEST_SET, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        ac.write(key, bin1, bin2, bin3);
        assertNotEquals(null, ac.read(key));
        ac.delete(key);
        assertNull(ac.read(key));
    }

    @Test
    void testDropDatabase() throws InterruptedException {
        Configuration c = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection ac = AerospikeConnection.connect(ConfigurationHelper.aerospikeHost(c), ConfigurationHelper.aerospikePort(c), ConfigurationHelper.aerospikeNamespace(c));
        Key key = new Key(ConfigurationHelper.aerospikeNamespace(c), TEST_SET, "foo");
        Bin bin1 = new Bin("name", "John Doe");
        Bin bin2 = new Bin("age", 32);
        Bin bin3 = new Bin("greeting", "Hello World!");
        ac.write(key, bin1, bin2, bin3);
        assertNotEquals(null, ac.read(key));
        ac.dropDatabase();
        assertNull(ac.read(key));
    }

    @Test
    void testAddRemoveIterateVertexIdList() {
        Configuration c = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection ac = AerospikeConnection.connect(
                ConfigurationHelper.aerospikeHost(c), ConfigurationHelper.aerospikePort(c), ConfigurationHelper.aerospikeNamespace(c));
        ac.writeElementId(FireflyVertex.class, 1L);
        ac.writeElementId(FireflyVertex.class, 2L);
        Iterator<Long> i = (Iterator<Long>) ac.readElementIds(FireflyVertex.class);
        assertEquals(1L, i.next());
        assertEquals(2L, i.next());
        ac.removeElementId(FireflyVertex.class, 2L);
        Iterator<Long> i2 = (Iterator<Long>) ac.readElementIds(FireflyVertex.class);
        assertEquals(1L, i2.next());
        assertFalse(i2.hasNext());
        ac.writeElementId(FireflyVertex.class, 3L);
        ac.writeElementId(FireflyVertex.class, 2L);
        Iterator<Long> i3 = (Iterator<Long>) ac.readElementIds(FireflyVertex.class);
        long last = 0L;
        while (i3.hasNext()) {
            long current = ((Number) i3.next()).longValue();
            assertTrue(current > last);
            last = current;
        }
    }

    @Test
    void testCounterOps() {
        Configuration c = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection ac = AerospikeConnection.connect(ConfigurationHelper.aerospikeHost(c), ConfigurationHelper.aerospikePort(c), ConfigurationHelper.aerospikeNamespace(c));
        ac.zeroIdCounter(GLOBAL);
        ac.incrementIdCounter(GLOBAL);
        assertEquals(1, ac.getIdCounter(GLOBAL));
        ac.decrementIdCounter(GLOBAL);
        assertEquals(0, ac.getIdCounter(GLOBAL));
        ac.incrementIdCounter(GLOBAL);
        ac.incrementIdCounter(GLOBAL);
        assertEquals(2, ac.getIdCounter(GLOBAL));
        ac.zeroIdCounter(GLOBAL);
        assertEquals(0, ac.getIdCounter(GLOBAL));
    }


}
