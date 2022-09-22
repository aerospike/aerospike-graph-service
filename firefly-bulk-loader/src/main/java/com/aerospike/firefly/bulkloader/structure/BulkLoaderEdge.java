package com.aerospike.firefly.bulkloader.structure;

import java.util.List;
import java.util.Map;

public class BulkLoaderEdge extends BulkLoaderElement {
    private final String fromId;
    private final String toId;

    public BulkLoaderEdge(final String id, final String label, final String fromId, final String toId,
                          final List<Map.Entry<String, Object>> properties) {
        super(id, label, properties);
        this.fromId = fromId;
        this.toId = toId;
    }

    public String getFromId() {
        return this.fromId;
    }

    public String getToId() {
        return this.toId;
    }

}
