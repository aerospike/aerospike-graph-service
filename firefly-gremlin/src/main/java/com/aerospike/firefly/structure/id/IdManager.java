package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface IdManager<T> {
    /**
     * Generate an identifier which should be unique to the {@link FireflyGraph} instance.
     * @param graph graph handle
     * @return next id
     */
    T getNextId(final FireflyGraph graph);

    /**
     * Convert an identifier to the type required by the manager.
     * @param id id to convert
     * @return converted id
     */
    T convert(final Object id);

    /**
     * Determine if an identifier is allowed by this manager given its type.
     * @param id id to check
     * @return is value allowed
     */
    boolean allow(final Object id);

    /**
     * Remove id from cache.
     * @param id id to remove
     */
    void removeFromCache(final Object id);
}
