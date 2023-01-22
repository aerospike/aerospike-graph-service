package com.aerospike.generator.beans.edges;

public class Edge {
    String label;
    Object from;
    Object to;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Object getFrom() {
        return from;
    }

    public void setFrom(Object from) {
        this.from = from;
    }

    public Object getTo() {
        return to;
    }

    public void setTo(Object to) {
        this.to = to;
    }
}
