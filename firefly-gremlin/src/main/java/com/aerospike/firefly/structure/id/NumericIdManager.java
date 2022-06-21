package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class NumericIdManager<T extends FireflyElement> implements IdManager<Long> {
    /**
     * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
     * {@link Long} and will also attempt to convert {@code String} values
     */
    private final String counterName;
    private final Set<Object> ids = new HashSet<>();
    private final Class<? extends FireflyElement> type;

    public NumericIdManager(Class<? extends FireflyElement> type, String counterName) {
        this.counterName = counterName;
        this.type = type;
    }

    private static String createErrorMessage(final Class<?> expectedType, final Object id) {
        return String.format("Expected an id that is convertible to %s but received %s - [%s]", expectedType, id.getClass(), id);
    }

    @Override
    public Long getNextId(FireflyGraph graph) {
        Long value = graph.getBaseGraph().incrementAndGetIdCounter(this.counterName);
        while (ids.contains(value)) {
            value = graph.getBaseGraph().incrementAndGetIdCounter(this.counterName);
        }
        ids.add(value);
        return value;
    }

    public void removeFromCache(Object id) {
        ids.remove(convert(id));
    }

    @Override
    public Long convert(Object id) {
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
    public boolean allow(Object id) {
        final boolean willAllow = AerospikeConnection.IdToDiskTypeMap.containsKey(id.getClass());
        return willAllow;
    }
}
