package com.aerospike.firefly.bulkloader.structure;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.List;
import java.util.Map;

public class BulkLoaderEdge extends BulkLoaderElement {
    private FireflyVertex from;
    private FireflyVertex to;

    public BulkLoaderEdge(long id, String label, FireflyVertex from, FireflyVertex to, List<Map.Entry<String, Object>> properties) {
        super(FireflyId.of(FireflyEdge.class, id), label, properties);
        this.from = from;
        this.to = to;
    }

    public FireflyVertex getFrom() {
        return this.from;
    };

    public FireflyVertex getTo() {
        return this.to;
    };
}
