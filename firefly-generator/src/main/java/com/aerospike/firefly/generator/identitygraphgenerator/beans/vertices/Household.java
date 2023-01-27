package com.aerospike.firefly.generator.identitygraphgenerator.beans.vertices;

import com.aerospike.firefly.generator.identitygraphgenerator.beans.Vertex;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class Household implements Vertex {
    Object id;
    String label;
    String street;
    String city;
    String state;
    Long zipcode;
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

    public void setStreet(String street) {
        this.street = street;
        this.valueMap.put("street", this.street);
    }

    public void setCity(String city) {
        this.city = city;
        this.valueMap.put("city", this.city);
    }

    public void setState(String state) {
        this.state = state;
        this.valueMap.put("state", this.state);
    }

    public void setZipcode(Long zipcode) {
        this.zipcode = zipcode;
        this.valueMap.put("zipcode", this.zipcode);
    }

    public HashMap<String, Object> getValueMap() {
        return valueMap;
    }

    @Override
    public Set<String> keys() {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        set.add("street");
        set.add("city");
        set.add("state");
        set.add("zipcode");
        return set;
    }
}
