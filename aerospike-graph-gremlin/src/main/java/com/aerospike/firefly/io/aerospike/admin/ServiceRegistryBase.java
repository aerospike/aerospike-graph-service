package com.aerospike.firefly.io.aerospike.admin;

import io.vertx.ext.web.Router;

import java.util.Set;

public abstract class ServiceRegistryBase {
    protected Set<AdminService> services;

    public void routeServices(final Router router) {
        for (final AdminService service : services) {
            if (service.needRouting())
                router.route(service.getPath()).handler(service.getHandler());
        }
    }
}
