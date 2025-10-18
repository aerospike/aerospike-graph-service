package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface IdManager<T> {
    /**
     * Generate an identifier which should be unique to the {@link FireflyGraph} instance.
     *
     * @param graph graph handle
     * @return next id
     */
    T getNextId(final FireflyGraph graph);

    /**
     * Recycle an id for reuse.
     *
     * @param id    id to recycle
     */
    void recycleId(final FireflyId id, final FireflyGraph graph, final boolean wasIdCommitted);
}
