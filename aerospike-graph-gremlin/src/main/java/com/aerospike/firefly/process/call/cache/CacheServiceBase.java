package com.aerospike.firefly.process.call.cache;

import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

/**
 * Base class for cache management services.
 * <p>
 * Provides common functionality for cache-related administrative operations.
 */
public abstract class CacheServiceBase<I, R> extends AdminService<I, R> {

    protected static final String MODE = "mode";
    protected static final String CACHE_WEIGHT = "cache_weight";

    public CacheServiceBase(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminNamespace() {
        return "cache";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }

    @Override
    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }
}
