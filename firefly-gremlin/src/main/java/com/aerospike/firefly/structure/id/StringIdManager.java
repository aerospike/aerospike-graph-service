package com.aerospike.firefly.structure.id;

import com.aerospike.client.Value;
import com.aerospike.client.util.Crypto;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.UUID;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class StringIdManager implements IdManager<String> {

    /**
     * Manages identifiers of type {@code String}. Will convert any class that extends from {@link String} to a
     * {@link String} and will also attempt to convert {@code String} values
     */
    protected final String counterName;
    private final FireflyGraph graph;

    public StringIdManager(final String counterName, final FireflyGraph graph) {
        this.counterName = counterName;
        this.graph = graph;
    }

    private static String createErrorMessage(final Class<?> expectedType, final Object id) {
        return String.format("Expected an id that is convertible to %s but received %s - [%s]", expectedType, id.getClass(), id);
    }

    /**
     * Create a String id
     *
     * @param graph graph handle
     * @return a new String id
     */
    @Override
    public String getNextId(final FireflyGraph graph) {
        return UUID.randomUUID().toString();
    }

    /**
     * Do we allow this id class to be used?
     *
     * @param id class of id to check
     * @return true if id class is allowed
     */
    @Override
    public boolean allow(final Class<?> id) {
        return AerospikeConnection.IdToDiskTypeMap.containsKey(id);
    }



}
