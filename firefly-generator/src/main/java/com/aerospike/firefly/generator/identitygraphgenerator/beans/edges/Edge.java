package com.aerospike.firefly.generator.identitygraphgenerator.beans.edges;

import java.util.HashMap;
import java.util.Map;

public class Edge {
    final Object id;
    final String label;
    final Object from;
    final Object to;
    final Map<String, String> properties;

    public Edge(Object id, String label, Object from, Object to, String... properties) {
        this.id = id;
        this.label = label;
        this.from = from;
        this.to = to;
        this.properties = new HashMap<>();
        if (properties.length % 2 != 0)
            throw new IllegalArgumentException("Properties must be in key/value pairs");
        for (int i = 0; i < properties.length; i += 2)
            this.properties.put(properties[i], properties[i + 1]);
    }

    public Object getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public Object getFrom() {
        return from;
    }

    public Object getTo() {
        return to;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
