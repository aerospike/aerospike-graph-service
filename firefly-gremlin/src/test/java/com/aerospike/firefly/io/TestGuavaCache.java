package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.firefly.io.impl.GuavaCache;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Calendar;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.Sets.TEST_SET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestGuavaCache {
    private static Configuration configuration;
    private static AerospikeConnection db;
    @BeforeClass
    public static void setup() {
        configuration = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        db = AerospikeConnection.connect(configuration);
    }

    @Before
    public void clearGraph() {
        try (final FireflyGraph graph = FireflyGraph.open(configuration)) {
            graph.traversal().V().drop().iterate();
        }
    }
    @AfterClass
    public static void cleanup() {
        db.getClient().truncate(null, db.getNamespace(), TEST_SET, Calendar.getInstance());
        db.close();
    }

    @Test
    public void testCacheBasic(){
        Cache cache = new GuavaCache(db);
        Key akey = new Key(db.getNamespace(), TEST_SET, "abcd");
        Record aval = cache.read(akey);
        assertNull(aval);
        cache.write(akey,new Bin("a", Value.get("b")));
        assertEquals(cache.read(akey).getString("a"),"b");
        assertEquals(db.getClient().get(new Policy(),akey).getString("a"),"b");
    }
}
