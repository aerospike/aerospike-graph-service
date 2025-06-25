package com.aerospike.firefly.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class FireflyFeatures implements Graph.Features {

    private final VertexProperty.Cardinality defaultCardinality;
    public FireflyFeatures(final VertexProperty.Cardinality defaultCardinality) {
        this.defaultCardinality = defaultCardinality;
    }

    @Override
    public GraphFeatures graph() {
        return new FireflyGraphFeatures();
    }

    /**
     * Gets the features related to "vertex" operation.
     */
    @Override
    public VertexFeatures vertex() {
        return new FireflyVertexFeatures(defaultCardinality);
    }

    /**
     * Gets the features related to "edge" operation.
     */
    @Override
    public EdgeFeatures edge() {
        return new FireflyEdgeFeatures();
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }
}
