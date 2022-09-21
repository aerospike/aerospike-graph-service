package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestNestedIndex extends AbstractFireflySuite {


    @Test
    public void creatIndexTest() {
        final String TEST_INDEX = "test_index";
        final String TEST_BIN = "test_bin";
        final String TEST_KEY = "test_key";
        final Map<String, String> mapDataFlatB = new HashMap<>() {{
            put("a", "b");
        }};
        final Map<String, String> mapDataFlatC = new HashMap<>() {{
            put("a", "c");
        }};
        final Map<String, Map<String, String>> mapDataNested = new HashMap<>() {{
            put("d", mapDataFlatB);
            put("e", mapDataFlatC);
        }};
        db.createIndex(new ArrayList<>(), db.TEST_SET, TEST_INDEX, TEST_BIN, IndexType.STRING, IndexCollectionType.MAPVALUES);
        Key key = new Key(db.getNamespace(), db.TEST_SET, TEST_KEY);
        db.write(key, new Bin(TEST_BIN, mapDataNested));
        Iterator<KeyRecord> ri = db.queryIndex(db.TEST_SET, TEST_INDEX, Filter.contains(TEST_BIN, IndexCollectionType.MAPVALUES, "c"));
        db.dropIndex(db.TEST_SET,TEST_INDEX);
        assertTrue(ri.hasNext());
    }


    @Override
    protected boolean clearData() {
        return true;
    }
}
