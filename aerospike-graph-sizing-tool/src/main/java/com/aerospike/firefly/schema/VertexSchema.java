package com.aerospike.firefly.schema;

import java.util.List;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class VertexSchema {
    public String label;
    public Long count;
    public List<PropertySchema> properties;
}
