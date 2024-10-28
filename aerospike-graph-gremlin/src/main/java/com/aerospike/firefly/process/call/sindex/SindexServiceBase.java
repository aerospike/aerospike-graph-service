package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

public abstract class SindexServiceBase<I, R> extends AdminService<I, R> {
    protected static final String ELEMENT_TYPE = "element_type";
    protected static final String PROPERTY_KEY = "property_key";

    public SindexServiceBase(final FireflyGraph graph) {
        super(graph);
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }

    @Override
    protected String getAdminNamespace() {
        return "index";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }
}
