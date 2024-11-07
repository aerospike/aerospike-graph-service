package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.firefly.process.call.AdministrativeInfoService;
import com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceRegistry;
import com.aerospike.firefly.process.call.metadata.MetadataServiceRegistry;
import com.aerospike.firefly.process.call.query.QueryServiceRegistry;
import com.aerospike.firefly.process.call.rbac.JwtServiceRegistry;
import com.aerospike.firefly.process.call.sindex.SindexServiceRegistry;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.ext.web.Router;

public class AdminServiceRegistry {

    private final SindexServiceRegistry sindexServiceRegistry;
    private final MetadataServiceRegistry metadataServiceRegistry;
    private final BulkLoaderServiceRegistry bulkLoaderServiceRegistry;
    private final AdministrativeInfoService administrativeInfoService;
    private final JwtServiceRegistry jwtServiceRegistry;
    private final QueryServiceRegistry queryServiceRegistry;

    public AdminServiceRegistry(final FireflyGraph firefly) {
        sindexServiceRegistry = new SindexServiceRegistry(firefly);
        metadataServiceRegistry = new MetadataServiceRegistry(firefly);
        bulkLoaderServiceRegistry = new BulkLoaderServiceRegistry(firefly);
        administrativeInfoService = new AdministrativeInfoService(firefly);
        jwtServiceRegistry = new JwtServiceRegistry(firefly);
        queryServiceRegistry = new QueryServiceRegistry(firefly);
    }

    public void appendHandlers(final Router router) {
        sindexServiceRegistry.routeServices(router);
        metadataServiceRegistry.routeServices(router);
        bulkLoaderServiceRegistry.routeServices(router);
        jwtServiceRegistry.routeServices(router);
        queryServiceRegistry.routeServices(router);
    }
}
