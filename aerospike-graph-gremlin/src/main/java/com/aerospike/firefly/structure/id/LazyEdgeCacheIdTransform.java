package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

public class LazyEdgeCacheIdTransform extends LazyIdTransform {

    protected LazyEdgeCacheIdTransform(final byte[] objectId, final FireflyGraph graph) {
        super(objectId, graph);
    }

    @Override
    public FireflyId transform() {
        if (this.id == null) {
            this.id = this.graph.getIdFactory().createCompositeEdgeId((byte[]) this.objectId);
        }
        return this.id;
    }
}
