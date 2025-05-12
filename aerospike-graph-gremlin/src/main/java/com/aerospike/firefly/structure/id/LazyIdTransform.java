package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.List;

public class LazyIdTransform {
    protected FireflyGraph graph;
    protected FireflyId id;
    protected Object objectId;

    public LazyIdTransform(final FireflyId id) {
        this.id = id;
    }

    protected LazyIdTransform(final Object objectId, final FireflyGraph graph) {
        this.objectId = objectId;
        this.graph = graph;
    }

    public FireflyId transform() {
        return id;
    }

    public static LazyIdTransform create(final Object objectId, final FireflyGraph graph,
                                         final Class<? extends LazyIdTransform> type) {
        if (LazyEdgeCacheIdTransform.class.isAssignableFrom(type)) {
            return new LazyEdgeCacheIdTransform((List<Object>) objectId, graph);
        } else if (LazyVertexPropertyIdTransform.class.isAssignableFrom(type)) {
            return new LazyVertexPropertyIdTransform(objectId, graph);
        } else {
            // This should never happen.
            throw new IllegalArgumentException("Invalid LazyIdTransform type: " + type);
        }
    }
}
