package com.aerospike.firefly.structure;

import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Optional;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyGraphVariables implements Graph.Variables {
    private final FireflyGraph graph;

    public FireflyGraphVariables(FireflyGraph graph) {
        this.graph = graph;
    }

    @Override
    public Set<String> keys() {
        return graph.getBaseGraph().readGraphVariableKeys();
    }

    @Override
    public <R> Optional<R> get(String key) {
        return Optional.ofNullable(graph.getBaseGraph().readGraphVariable(key));
    }

    @Override
    public void set(String key, Object value) {
        if (null == value)
            throw Graph.Variables.Exceptions.variableValueCanNotBeNull();
        if (null == key || key.isEmpty())
            throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        FireflyHelper.validateGraphVariableValue(value);
        graph.getBaseGraph().writeGraphVariable(key, value);
    }

    @Override
    public void remove(String key) {
        graph.getBaseGraph().removeGraphVariable(key);
    }

    @Override
    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
