package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.process.call.sindex.SindexServiceBase;
import com.aerospike.firefly.process.call.sindex.SindexServiceCardinality;
import com.aerospike.firefly.process.call.sindex.SindexServiceCreate;
import com.aerospike.firefly.process.call.sindex.SindexServiceDrop;
import com.aerospike.firefly.process.call.sindex.SindexServiceList;
import com.aerospike.firefly.process.call.sindex.SindexServiceStatus;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.ext.web.Router;

import java.util.Set;

public abstract class MetadataServiceBase<I, R> extends AdminServiceRegistry<I, R> {
    protected FireflyGraph firefly;

    public MetadataServiceBase(final FireflyGraph firefly) {
        super(firefly);
        this.firefly = firefly;
    }

    @Override
    protected String getAdminNamespace() {
        return "metadata";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }

    protected static Set<MetadataServiceBase> services;

    public static void registerMetadataServices(final FireflyGraph firefly) {
        synchronized (MetadataServiceBase.class) {
            Set.of(
                    new MetadataServiceUsage<>(firefly),
                    new MetadataServiceSummary<>(firefly)
            ).forEach(firefly.getServiceRegistry()::registerService);

            // HTTP routing comes up before firefly. Need to latch firefly into the services.
            if (services != null) {
                services.forEach(s -> s.firefly = firefly);
            }
        }
    }

    public static void routeMetadataServices(final Router router) {
        synchronized (MetadataServiceBase.class) {
            // Latch services so they can be updated later.
            if (services == null) {
                services = Set.of(
                        new MetadataServiceUsage<>(null),
                        new MetadataServiceSummary<>(null));
                services.forEach(service -> router.route(service.getPath()).handler(service.getHandler()));
            }
        }
    }
}
