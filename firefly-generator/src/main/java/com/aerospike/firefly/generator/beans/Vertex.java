package com.aerospike.firefly.generator.beans;

import java.util.HashMap;
import java.util.Set;

public interface Vertex {
    public Long getId();

    public void setId(Long id);

    public String getLabel();

    public void setLabel(String label);

    public HashMap<String, Object> getValueMap() ;

    public Set<String> keys();
}
