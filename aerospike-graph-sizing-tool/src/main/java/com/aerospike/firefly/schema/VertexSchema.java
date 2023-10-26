package com.aerospike.firefly.schema;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class VertexSchema {
    public String label;
    public Long count;
    public List<PropertySchema> properties;

    public VertexSchema() {
        properties = new ArrayList<>();
    }
}
