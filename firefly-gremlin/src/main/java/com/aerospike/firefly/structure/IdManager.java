package com.aerospike.firefly.structure;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface IdManager<T> {
    /**
     * Generate an identifier which should be unique to the {@link FireflyGraph} instance.
     */
    T getNextId(final FireflyGraph graph);

    /**
     * Convert an identifier to the type required by the manager.
     */
    T convert(final Object id);

    /**
     * Determine if an identifier is allowed by this manager given its type.
     */
    boolean allow(final Object id);
}
