package com.aerospike.firefly.process.call.query;

import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.ext.web.Router;

import java.util.Set;

public abstract class QueryServiceBase<I, R> extends AdminService<I, R> {

    public QueryServiceBase(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminNamespace() {
        return "query";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }
}
