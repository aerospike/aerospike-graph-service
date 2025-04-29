package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.List;

public class LazyEdgeCacheIdTransform extends LazyIdTransform {

    protected LazyEdgeCacheIdTransform(final List<Object> objectId, final FireflyGraph graph) {
        super(objectId, graph);
    }

    @Override
    public FireflyId transform() {
        if (this.id == null) {
            this.id = this.graph.getIdFactory().createCompositeEdgeId((List<Object>) this.objectId);
        }
        return this.id;
    }
}
