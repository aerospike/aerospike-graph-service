package com.aerospike.firefly.generator.identitygraphgenerator.beans.edges;

public class Edge {
    Object id;
    String label;
    Object from;
    Object to;

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setFrom(Object from) {
        this.from = from;
    }

    public void setTo(Object to) {
        this.to = to;
    }
}
