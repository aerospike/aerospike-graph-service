package com.aerospike.firefly.bulkloader.structure;

import com.aerospike.firefly.structure.id.FireflyId;

import java.util.List;
import java.util.Map;

public abstract class BulkLoaderElement {
    protected final FireflyId id;
    protected final String label;
    protected final List<Map.Entry<String, Object>> properties;

    protected BulkLoaderElement(final FireflyId id, final String label,
                                final List<Map.Entry<String, Object>> properties) {
        this.id = id;
        this.label = label;
        this.properties = properties;
    }

    protected void AddProperty(final Map.Entry<String, Object> property) {
        this.properties.add(property);
    }

    public FireflyId getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public List<Map.Entry<String, Object>> getProperties() {
        return this.properties;
    }
}
