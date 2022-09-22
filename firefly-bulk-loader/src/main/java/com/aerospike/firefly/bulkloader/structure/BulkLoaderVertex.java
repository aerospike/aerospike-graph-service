package com.aerospike.firefly.bulkloader.structure;

import java.util.List;
import java.util.Map;

public class BulkLoaderVertex extends BulkLoaderElement {
    public BulkLoaderVertex(final String id, final String label, final List<Map.Entry<String, Object>> properties) {
        super(id, label, properties);
    }
}
