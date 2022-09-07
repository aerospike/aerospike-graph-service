package com.aerospike.firefly.bulkloader.structure;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.List;
import java.util.Map;

public class BulkLoaderEdge extends BulkLoaderElement {
    private final FireflyVertex from;
    private final FireflyVertex to;

    public BulkLoaderEdge(final long id, final String label, final FireflyVertex from, final FireflyVertex to,
                          final List<Map.Entry<String, Object>> properties) {
        super(FireflyId.of(FireflyEdge.class, id), label, properties);
        this.from = from;
        this.to = to;
    }

    public FireflyVertex getFrom() {
        return this.from;
    }

    public FireflyVertex getTo() {
        return this.to;
    }

}
