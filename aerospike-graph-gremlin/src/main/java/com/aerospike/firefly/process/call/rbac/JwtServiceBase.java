package com.aerospike.firefly.process.call.rbac;

import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

public abstract class JwtServiceBase<I, R> extends AdminService<I, R> {

    public JwtServiceBase(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminNamespace() {
        return "rbac-jwt";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }
}
