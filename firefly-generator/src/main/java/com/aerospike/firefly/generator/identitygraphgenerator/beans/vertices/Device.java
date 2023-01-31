package com.aerospike.firefly.generator.identitygraphgenerator.beans.vertices;

import com.aerospike.firefly.generator.identitygraphgenerator.beans.Vertex;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class Device implements Vertex {
    Object id;
    String label;
    String macAddress;
    String make;
    String model;
    HashMap<String, Object> valueMap = new HashMap<>();


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

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
        this.valueMap.put("macAddress", this.macAddress);
    }

    public void setMake(String make) {
        this.make = make;
        this.valueMap.put("make", this.make);
    }

    public void setModel(String model) {
        this.model = model;
        this.valueMap.put("model", this.model);
    }

    public HashMap<String, Object> getValueMap() {
        return valueMap;
    }

    @Override
    public Set<String> keys() {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        set.add("macAddress");
        set.add("make");
        set.add("model");
        return set;
    }
}
