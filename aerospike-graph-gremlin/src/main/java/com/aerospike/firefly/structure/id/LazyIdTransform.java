package com.aerospike.firefly.structure.id;


import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;

public class LazyIdTransform {
    final FireflyGraph graph;
    FireflyId id = null;
    Object objectId = null;
    Class<? extends FireflyElement> type = null;


    public LazyIdTransform(final FireflyId id, final FireflyGraph graph) {
        this.id = id;
        this.graph = graph;
    }

    public LazyIdTransform(final Object objectId, final FireflyGraph graph, final Class<? extends FireflyElement> type) {
        this.objectId = objectId;
        this.graph = graph;
        this.type = type;
    }

    public FireflyId transform() {
        if (id == null) {
            // Default vertex could be vertex property tho.
            id = graph.getIdFactory().createId(objectId, type);
        }
        return id;
    }
}