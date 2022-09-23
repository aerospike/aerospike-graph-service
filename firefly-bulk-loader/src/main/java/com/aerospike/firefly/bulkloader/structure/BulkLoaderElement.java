package com.aerospike.firefly.bulkloader.structure;

import java.util.List;
import java.util.Map;

public abstract class BulkLoaderElement {
    protected final String id;
    protected final String label;
    protected final List<Map.Entry<String, Object>> properties;

    protected BulkLoaderElement(final String id, final String label,
                                final List<Map.Entry<String, Object>> properties) {
        this.id = id;
        this.label = label;
        this.properties = properties;
    }

    public String getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public List<Map.Entry<String, Object>> getProperties() {
        return this.properties;
    }
}
