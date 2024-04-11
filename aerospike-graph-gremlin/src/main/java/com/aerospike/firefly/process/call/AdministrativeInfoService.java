package com.aerospike.firefly.process.call;

import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.service.Service;

import java.util.Map;

public class AdministrativeInfoService extends AdminServiceRegistry {
    public AdministrativeInfoService(final FireflyGraph graph) {
        super(graph);
    }

    public static void registerAdministrativeService(final FireflyGraph graph) {
        synchronized (AdministrativeInfoService.class) {
            graph.getServiceRegistry().registerService(new AdministrativeInfoService(graph));
        }
    }

    @Override
    public Service createService(final boolean isStart, final Map params) {
        return this;
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }

    @Override
    protected String getAdminNamespace() {
        return null;
    }

    @Override
    protected String getAdminServiceName() {
        return "info";
    }

    @Override
    protected String usage(final Map params) {
        throw new UnsupportedOperationException("This service should never be executed.");
    }

    @Override
    protected boolean sanitize(final Map params) {
        throw new UnsupportedOperationException("This service should never be executed.");
    }

    @Override
    protected Object execute(final Map params) {
        throw new UnsupportedOperationException("This service should never be executed.");
    }

    @Override
    protected void auditLog(final Map params) {
        throw new UnsupportedOperationException("This service should never be executed.");
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }
}
