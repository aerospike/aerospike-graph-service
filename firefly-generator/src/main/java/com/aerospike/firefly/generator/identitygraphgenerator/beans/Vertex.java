package com.aerospike.firefly.generator.identitygraphgenerator.beans;

import java.util.HashMap;
import java.util.Set;

public interface Vertex {
    public Object getId();

    public void setId(Object id);

    public String getLabel();

    public void setLabel(String label);

    public HashMap<String, Object> getValueMap() ;

    public Set<String> keys();
}
