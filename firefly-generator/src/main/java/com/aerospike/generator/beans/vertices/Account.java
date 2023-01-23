package com.aerospike.generator.beans.vertices;
import org.apache.tinkerpop.gremlin.structure.*;

public class Account {
    String label;
    String number;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }
}
