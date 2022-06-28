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
     * Determine if an identifier's class is allowed by this manager given its type.
     * @param id class of id to check
     * @return is value allowed
     */
    boolean allow(final Class<?> id);

    /**
     * Add id to cache. Only used in user supplied ids.
     * @param id id to add
     */
    void addToCache(final Object id);

    /**
     * Remove id from cache.
     * @param id id to remove
     */
    void removeFromCache(final Object id);
}
