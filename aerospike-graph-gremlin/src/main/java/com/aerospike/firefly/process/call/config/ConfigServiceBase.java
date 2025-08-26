package com.aerospike.firefly.process.call.config;

import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

public abstract class ConfigServiceBase<I, R> extends AdminService<I, R> {

    public ConfigServiceBase(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminNamespace() {
        return "config";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.READ;
    }
}
