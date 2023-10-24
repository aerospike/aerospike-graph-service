package com.aerospike.firefly.schema;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class PropertySchema {
    public String key;
    public String type;
    public Long size;
    public double likelihood;
    public boolean sindexed;

    PropertySchema() {
        likelihood = 1.0;
        sindexed = false;
    }
}
