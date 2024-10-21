package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

public abstract class BulkLoaderServiceBase<I, R> extends AdminService<I, R> {

    public BulkLoaderServiceBase(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminNamespace() {
        return "bulk-load";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graphloader";
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.READ_WRITE;
    }
}
