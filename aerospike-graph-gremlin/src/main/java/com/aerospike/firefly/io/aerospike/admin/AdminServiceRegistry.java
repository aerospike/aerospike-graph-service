package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.firefly.process.call.AdministrativeInfoService;
import com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceRegistry;
import com.aerospike.firefly.process.call.metadata.MetadataServiceRegistry;
import com.aerospike.firefly.process.call.query.QueryServiceRegistry;
import com.aerospike.firefly.process.call.rbac.JwtServiceRegistry;
import com.aerospike.firefly.process.call.sindex.SindexServiceRegistry;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.ext.web.Router;

import java.util.Optional;

public class AdminServiceRegistry {

    private final Optional<SindexServiceRegistry> sindexServiceRegistry;
    private final Optional<MetadataServiceRegistry> metadataServiceRegistry;
    private final Optional<BulkLoaderServiceRegistry> bulkLoaderServiceRegistry;
    private final Optional<AdministrativeInfoService> administrativeInfoService;
    private final Optional<JwtServiceRegistry> jwtServiceRegistry;
    private final Optional<QueryServiceRegistry> queryServiceRegistry;

    public AdminServiceRegistry(final FireflyGraph firefly,
                                final boolean isWarmup,
                                final boolean isBulkLoader,
                                final boolean isOlap) {
        if (isWarmup || isBulkLoader) {
            sindexServiceRegistry = Optional.empty();
            metadataServiceRegistry = Optional.empty();
            bulkLoaderServiceRegistry = Optional.empty();
            administrativeInfoService = Optional.empty();
            jwtServiceRegistry = Optional.empty();
            queryServiceRegistry = Optional.empty();
            return;
        }
        sindexServiceRegistry = Optional.of(new SindexServiceRegistry(firefly));
        metadataServiceRegistry = Optional.of(new MetadataServiceRegistry(firefly));
        bulkLoaderServiceRegistry = isOlap ? Optional.empty() : Optional.of(new BulkLoaderServiceRegistry(firefly));
        administrativeInfoService = Optional.of(new AdministrativeInfoService(firefly));
        jwtServiceRegistry = Optional.of(new JwtServiceRegistry(firefly));
        queryServiceRegistry = Optional.of(new QueryServiceRegistry(firefly));
    }

    public void appendHandlers(final Router router) {
        sindexServiceRegistry.ifPresent(serviceRegistry -> serviceRegistry.routeServices(router));
        metadataServiceRegistry.ifPresent(serviceRegistry -> serviceRegistry.routeServices(router));
        bulkLoaderServiceRegistry.ifPresent(serviceRegistry -> serviceRegistry.routeServices(router));
        jwtServiceRegistry.ifPresent(serviceRegistry -> serviceRegistry.routeServices(router));
        queryServiceRegistry.ifPresent(serviceRegistry -> serviceRegistry.routeServices(router));
    }
}
