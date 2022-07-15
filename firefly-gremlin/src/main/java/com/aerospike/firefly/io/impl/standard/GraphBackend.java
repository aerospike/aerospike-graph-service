package com.aerospike.firefly.io.impl.standard;

import com.aerospike.firefly.io.AbstractBackend;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.Backend;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphBackend extends AbstractBackend implements Backend.Graph {
    public GraphBackend(AerospikeConnection db) {
        super(db);
    }

    /**
     * Return a Graph variable value by name
     *
     * @param key Graph variable key
     * @param <V> type
     * @return Graph variable value
     */
    @Override
    public <V> V readGraphVariable(final String key) {
        return db.readTypeHintedValueFromMap(db.GRAPH_VARIABLES_SET, FireflyId.of(null, db.GRAPH_VARIABLES_RECORD), db.GRAPH_VARIABLES_MAP, key);
    }

    /**
     * Return a Set of all the Graph variable names
     *
     * @return Set of Graph variable keys
     */
    @Override
    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.GRAPH_VARIABLES_SET, FireflyId.of(null, db.GRAPH_VARIABLES_RECORD));
        if (fireflyRecord == null)
            return new HashSet<>();
        final Map<String, ?> m = (Map<String, ?>) fireflyRecord.record.getMap(db.GRAPH_VARIABLES_MAP);
        return m.keySet();
    }

    /**
     * Write a Graph variable
     *
     * @param key   Graph variable key
     * @param value Graph variable value to write
     * @param <V>   Graph variable value type
     */
    @Override
    public <V> void writeGraphVariable(final String key, final V value) {
        db.writeTypeHintedValueToMap(db.GRAPH_VARIABLES_SET, FireflyId.of(null, db.GRAPH_VARIABLES_RECORD), db.GRAPH_VARIABLES_MAP, key, value);
    }

    /**
     * Remove a Graph variable
     *
     * @param key Graph variable key to remove
     * @param <V> Graph variable type
     */
    @Override
    public <V> void removeGraphVariable(final String key) {
        db.removeTypeHintedValueFromMap(db.GRAPH_VARIABLES_SET, FireflyId.of(null, db.GRAPH_VARIABLES_RECORD), db.GRAPH_VARIABLES_MAP, key);
    }

}
