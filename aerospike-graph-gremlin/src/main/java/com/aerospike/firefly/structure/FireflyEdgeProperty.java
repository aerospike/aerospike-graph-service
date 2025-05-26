package com.aerospike.firefly.structure;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyEdgeProperty<V> extends FireflyProperty<V> {
    private final FireflyGraph graph;
    private final FireflyEdge edge;

    /**
     * Constructor for RelationalProperty.
     *
     * @param graph   Graph that property exists in.
     * @param edge    Edge that property exists on.
     * @param key     Key of property.
     * @param value   Value of property.
     */
    public FireflyEdgeProperty(final FireflyGraph graph, final FireflyEdge edge, final String key, final V value) {
        super(edge, key, value);
        this.graph = graph;
        this.edge = edge;
    }

    /**
     * Remove this property from the graph.
     */
    @Override
    public void remove() {
        graph.getAerospikeOperations().removeEdgeProperty(this);
    }

    public FireflyEdge edge() {
        return edge;
    }
}
