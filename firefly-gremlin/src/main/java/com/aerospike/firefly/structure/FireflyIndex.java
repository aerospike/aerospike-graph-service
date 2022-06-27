package com.aerospike.firefly.structure;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
final class FireflyIndex<T extends FireflyElement> {

    protected Map<String, Map<Object, Set<T>>> index = new ConcurrentHashMap<>();
    protected final Class<T> indexClass;
    private final Set<String> indexedKeys = new HashSet<>();
    private final FireflyGraph graph;

    public FireflyIndex(final FireflyGraph graph, final Class<T> indexClass) {
        this.graph = graph;
        this.indexClass = indexClass;
    }

    public void createKeyIndex(final String key) {
        //@todo index types
        this.graph.getBaseGraph().createBinIndex(indexClass,key, IndexType.NUMERIC, IndexCollectionType.DEFAULT);
    }

    public void dropKeyIndex(final String key) {
        this.graph.getBaseGraph().dropBinIndex(indexClass, key);
    }

    public static Object indexable(final Object obj) {
        return null == obj ? FireflyIndex.IndexedNull.instance() : obj;
    }

    public Set<String> getIndexedKeys() {
        return this.indexedKeys;
    }

    public static final class IndexedNull {
        private static final FireflyIndex.IndexedNull inst = new FireflyIndex.IndexedNull();

        private IndexedNull() {}

        static FireflyIndex.IndexedNull instance() {
            return inst;
        }

        @Override
        public int hashCode() {
            return 751912123;
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof FireflyIndex.IndexedNull;
        }
    }
}
