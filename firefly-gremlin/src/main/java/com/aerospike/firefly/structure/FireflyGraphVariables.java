package com.aerospike.firefly.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyGraphVariables implements Graph.Variables{
    private final FireflyGraph graph;

    public FireflyGraphVariables(FireflyGraph graph){
        this.graph = graph;
    }
    @Override
    public Set<String> keys() {
        return graph.db.readGraphVariableKeys();
    }

    @Override
    public <R> Optional<R> get(String key) {
        return Optional.ofNullable(graph.db.readGraphVariable(key));
    }

    @Override
    public void set(String key, Object value) {
        graph.db.writeGraphVariable(key,value);
    }

    @Override
    public void remove(String key) {
        graph.db.removeGraphVariable(key);
    }
}
