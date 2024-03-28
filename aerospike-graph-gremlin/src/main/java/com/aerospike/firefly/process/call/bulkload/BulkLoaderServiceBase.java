package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.structure.FireflyGraph;

public abstract class BulkLoaderServiceBase<I, R> extends AdminServiceRegistry<I, R> {
    protected FireflyGraph graph;

    public BulkLoaderServiceBase(final FireflyGraph graph) {
        super(graph);
        this.graph = graph;
    }

    @Override
    protected String getAdminNamespace() {
        return "aerospike.graphloader.bulk-load.load";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graphloader";
    }
}
