package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class NumericIdManager<T extends FireflyElement> implements IdManager<Long> {
    /**
     * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
     * {@link Long} and will also attempt to convert {@code String} values
     */
    protected final String counterName;
    private final Class<? extends FireflyElement> type;

    public NumericIdManager(final Class<? extends FireflyElement> type, final String counterName) {
        this.counterName = counterName;
        this.type = type;
    }

    private static String createErrorMessage(final Class<?> expectedType, final Object id) {
        return String.format("Expected an id that is convertible to %s but received %s - [%s]", expectedType, id.getClass(), id);
    }

    @Override
    public Long getNextId(final FireflyGraph graph) {
        return graph.getBaseGraph().decrementIdCounter(this.counterName);
    }

    public static Long convert(Object id) {
        if (id != null)
            if (Element.class.isAssignableFrom(id.getClass()))
                id = ((Element) id).id();
            else if (FireflyId.class.isAssignableFrom(id.getClass()))
                id = ((FireflyId) id).value();
        if (null == id)
            return null;
        else if (id instanceof Number)
            return ((Number) id).longValue();
        else if (id instanceof String) {
            try {
                return Long.parseLong((String) id);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException(createErrorMessage(Long.class, id));
            }
        } else
            throw new IllegalArgumentException(createErrorMessage(Long.class, id));
    }

    @Override
    public boolean allow(final Class<?> id) {
        return AerospikeConnection.IdToDiskTypeMap.containsKey(id);
    }
}
