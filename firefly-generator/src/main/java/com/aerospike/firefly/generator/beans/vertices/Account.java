package com.aerospike.firefly.generator.beans.vertices;

import com.aerospike.firefly.generator.beans.Vertex;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class Account implements Vertex {
    Long id;
    String label;
    String number;
    HashMap<String, Object> valueMap = new HashMap<>();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setNumber(String number) {
        this.number = number;
        this.valueMap.put("number", this.number);
    }

    public HashMap<String, Object> getValueMap() {
        return valueMap;
    }

    @Override
    public Set<String> keys() {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        set.add("number");
        return set;
    }
}
