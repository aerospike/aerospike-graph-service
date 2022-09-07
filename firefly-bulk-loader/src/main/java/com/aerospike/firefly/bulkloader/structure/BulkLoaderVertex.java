package com.aerospike.firefly.bulkloader.structure;

import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BulkLoaderVertex extends BulkLoaderElement {
    BulkLoaderVertex(final long id, final String label) {
        this(id, label, new ArrayList<>());
    }

    public BulkLoaderVertex(final long id, final String label, final List<Map.Entry<String, Object>> properties) {
        super(FireflyId.of(FireflyVertex.class, id), label, properties);
    }
}
