package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class NumericIdManager implements IdManager<Long> {
    /**
     * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
     * {@link Long} and will also attempt to convert {@code String} values
     */
    protected final String counterName;

    public NumericIdManager(final String counterName) {
        this.counterName = counterName;
    }

    private static String createErrorMessage(final Class<?> expectedType, final Object id) {
        return String.format("Expected an id that is convertible to %s but received %s - [%s]", expectedType, id.getClass(), id);
    }

    @Override
    public Long getNextId(final FireflyGraph graph) {
        return graph.getBaseGraph().decrementIdCounter(this.counterName);
    }

    @Override
    public boolean allow(final Class<?> id) {
        return AerospikeConnection.IdToDiskTypeMap.containsKey(id);
    }
}
