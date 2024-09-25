package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;

public class LazyVertexPropertyIdTransform extends LazyIdTransform {

    protected LazyVertexPropertyIdTransform(final Object objectId, final FireflyGraph graph) {
        super(objectId, graph);
    }

    @Override
    public FireflyId transform() {
        if (this.id == null) {
            this.id = this.graph.getIdFactory().createVertexPropertyId(this.objectId);
        }
        return this.id;
    }
}
