package com.aerospike.firefly.process.call.expressionindex;

import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

public abstract class ExpressionIndexServiceBase<I, R> extends AdminService<I, R> {

    public ExpressionIndexServiceBase(final FireflyGraph graph) {
        super(graph);
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }

    @Override
    protected String getAdminNamespace() {
        return "compound-index";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }
}
