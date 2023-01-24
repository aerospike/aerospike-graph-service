package com.aerospike.firefly.generator.beans.vertices;

import com.aerospike.firefly.generator.beans.Vertex;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class Person implements Vertex {
    Long id;
    String label;
    String firstName;
    String lastName;
    Long ssn;
    String email;
    Long phone;
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

    public void setFirstName(String firstName) {
        this.firstName = firstName;
        this.valueMap.put("firstName", this.firstName);
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
        this.valueMap.put("lastName", this.lastName);
    }

    public void setSsn(Long ssn) {
        this.ssn = ssn;
        this.valueMap.put("ssn", this.ssn);
    }

    public void setEmail(String email) {
        this.email = email;
        this.valueMap.put("email", this.email);
    }

    public void setPhone(Long phone) {
        this.phone = phone;
        this.valueMap.put("phone", this.phone);
    }

    public HashMap<String, Object> getValueMap() {
        return valueMap;
    }

    @Override
    public Set<String> keys() {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        set.add("firstName");
        set.add("lastName");
        set.add("ssn");
        set.add("email");
        set.add("phone");
        return set;
    }
}
