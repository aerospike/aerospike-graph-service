package com.aerospike.firefly.process.call;

import com.aerospike.firefly.bulkloader.BulkLoaderCallEntryPoint;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Map;
import java.util.Set;

public class FireflyBulkLoaderServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {

    private static final Set<String> ALLOWED_KEYS = Set.of("config", "aws", "vertices", "edges");

    @Override
    public String getName() {
        return "bulk-load";
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return null;
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return this;
    }

    @Override
    public Type getType() {
        return null;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        final BulkLoad bulkLoad = new BulkLoaderCallEntryPoint();
        String configPath = "";
        boolean aws = false;
        boolean vertices = true;
        boolean edges = true;

        // Get any provided parameters that are not allowed.
        final Sets.SetView<String> diff = Sets.difference(params.keySet(), ALLOWED_KEYS);
        if (!diff.isEmpty()) {
            throw new IllegalArgumentException("The bulk loader expects the following parameters: " + ALLOWED_KEYS + ". " +
                    "The following provided parameters are not allowed: " + diff + ".");
        }

        if (params.containsKey("config")) {
            configPath = getStringFromObject(params.get("config"), "config");
        }

        if (params.containsKey("aws")) {
            aws = getBooleanFromObject(params.get("aws"), "aws");
        }

        // The way specifying vertices or edges is that:
        // If you specify neither, both are loaded.
        // If you specify both as true, then both element types are loaded.
        // If you specify both as false, an error is returned.
        // If you specify 1 as true, that is the only element type loaded.
        // If you specify 1 as false, then only the other element type is loaded.
        if (params.containsKey("vertices") && params.containsKey("edges")) {
            vertices = getBooleanFromObject(params.get("vertices"), "vertices");
            edges = getBooleanFromObject(params.get("edges"), "edges");
        } else if (params.containsKey("vertices")) {
            vertices = getBooleanFromObject(params.get("vertices"), "vertices");
            edges = !vertices;
        } else if (params.containsKey("edges")) {
            edges = getBooleanFromObject(params.get("edges"), "edges");
            vertices = !edges;
        }

        if (!vertices && !edges) {
            throw new IllegalArgumentException("Either 'vertices' or 'edges' must be set to true.");
        }
        if ("".equals(configPath)) {
            throw new IllegalArgumentException("Bulk load config path ('config') must be set to a non-empty String value.");
        }

        bulkLoad.perform(configPath, aws, vertices, edges);

        // Return success if it worked, otherwise it will return an exception.
        return FireflyCloseableIteratorUtils.of((R)"Success");
    }

    private boolean getBooleanFromObject(final Object obj, final String name) {
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        }
        if (obj != null) {
            throw new IllegalArgumentException("Expected bulk loader flag '" + name + "' to be set to a boolean value. " +
                    "Instead value was set with type '" + obj.getClass().getName() + "'.");
        } else {
            throw new IllegalArgumentException("Expected bulk loader flag '" + name + "' to be set to a boolean value. " +
                    "Instead value was null.");
        }
    }

    private String getStringFromObject(final Object obj, final String name) {
        if (obj instanceof String) {
            return (String) obj;
        }
        if (obj != null) {
            throw new IllegalArgumentException("Expected bulk loader flag '" + name + "' to be set to a String value. " +
                    "Instead value was set with type '" + obj.getClass().getName() + "'.");
        } else {
            throw new IllegalArgumentException("Expected bulk loader flag '" + name + "' to be set to a String value. " +
                    "Instead value was null.");
        }
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }


    public interface BulkLoad {
        void perform(final String configPath, final boolean aws, final boolean verticesOnly, final boolean edgesOnly);
    }
}
