package com.aerospike.firefly.schema;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class PropertySchema {
    public String key;
    public String type;
    public Number size;
    public Number count;
    public Number likelihood;
    public boolean sindexed;

    public PropertySchema() {
        likelihood = 1.0;
        sindexed = false;
    }
}
