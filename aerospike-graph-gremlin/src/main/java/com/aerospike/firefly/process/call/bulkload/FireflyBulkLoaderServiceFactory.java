package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.process.call.bulkload.utils.FireflyBulkLoaderInterface;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DATAFRAME_STORAGE_TYPE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_WRITE_BUFFER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ENABLE_DATAFRAME_CACHING;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEY_TO_CMD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.SAMPLING_PERCENTAGE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.SPARK_LOG_LEVEL;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERTEX_WRITE_BUFFER;
import static com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser.DRY_RUN;
import static com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser.LOCAL_MODE;
import static com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser.VERIFY_EDGE;
import static com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser.VERIFY_VERTEX;
import static com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser.WRITE_EDGE;
import static com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser.WRITE_VERTEX;

public class FireflyBulkLoaderServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    private static final String DEFAULT_CONFIG_PATH = "/opt/aerospike-firefly/conf/firefly-graph.properties";
    private static final String CONFIG = "aerospike.graphloader.config";
    private static final String VERTICES = "vertices";
    private static final String EDGES = "edges";
    private static final String DRYRUN = "dryrun";
    private static final Map<String, String> KEY_TO_ARG = new HashMap<>();
    private static final Set<String> INTERNAL_CONFIGS = Set.of(
            VERTICES,
            EDGES,
            DRYRUN,
            CONFIG,
            "aerospike.graphloader.s3-endpoint",
            SPARK_LOG_LEVEL,
            ENABLE_DATAFRAME_CACHING,
            DATAFRAME_STORAGE_TYPE
    );

    private static final Set<String> BOOLEAN_KEYS = Set.of(
            KEEP_PROVIDED_EDGE_ID_AS_PROPERTY,
            ENABLE_DATAFRAME_CACHING
    );

    private static final Set<String> NUMBER_KEYS = Set.of(
            SAMPLING_PERCENTAGE,
            VERTEX_WRITE_BUFFER,
            EDGE_WRITE_BUFFER
    );

    static {
        KEY_TO_ARG.put(VERTICES, null);
        KEY_TO_ARG.put(EDGES, null);
        KEY_TO_ARG.put(DRYRUN, null);
        KEY_TO_ARG.put(CONFIG, "c");
        KEY_TO_ARG.put("aerospike.graphloader.remote.user", "u");
        KEY_TO_ARG.put("aerospike.graphloader.remote.passkey", "p");
        KEY_TO_ARG.put("aerospike.graphloader.gcs-keyfile", "gck");
        KEY_TO_ARG.put("aerospike.graphloader.gcs-email", "gem");
        KEY_TO_ARG.putAll(KEY_TO_CMD);
        KEY_TO_ARG.put("aerospike.graphloader.s3-endpoint", "s3e");
    }

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
        // Get any provided parameters that are not allowed.
        final Sets.SetView<String> diff = Sets.difference(params.keySet(), KEY_TO_ARG.keySet());
        if (!diff.isEmpty()) {
            final Sets.SetView<String> publicParams = Sets.symmetricDifference(INTERNAL_CONFIGS, KEY_TO_ARG.keySet());
            throw new IllegalArgumentException("The bulk loader allows the following parameters: " + publicParams + ". " +
                    "The following provided parameters are not allowed: " + diff + ".");
        }

        final List<String> args = new ArrayList<>();
        final Map<String, Object> mutableParams = new HashMap();
        mutableParams.putAll(params);
        if (!mutableParams.containsKey(CONFIG)) {
            mutableParams.put(CONFIG, DEFAULT_CONFIG_PATH);
        }
        boolean vertices = true;
        boolean edges = true;
        boolean dryrun = false;

        // The way specifying vertices or edges is that:
        // If you specify neither, both are loaded.
        // If you specify both as true, then both element types are loaded.
        // If you specify both as false, an error is returned.
        // If you specify 1 as true, that is the only element type loaded.
        // If you specify 1 as false, then only the other element type is loaded.
        if (mutableParams.containsKey("vertices") && mutableParams.containsKey("edges")) {
            vertices = getBooleanFromObject(mutableParams.get("vertices"), "vertices");
            edges = getBooleanFromObject(mutableParams.get("edges"), "edges");
        } else if (mutableParams.containsKey("vertices")) {
            vertices = getBooleanFromObject(mutableParams.get("vertices"), "vertices");
            edges = !vertices;
        } else if (mutableParams.containsKey("edges")) {
            edges = getBooleanFromObject(mutableParams.get("edges"), "edges");
            vertices = !edges;
        }

        if (!vertices && !edges) {
            throw new IllegalArgumentException("Either 'vertices' or 'edges' must be set to true.");
        }

        if (mutableParams.containsKey("dryrun")) {
            dryrun = getBooleanFromObject(mutableParams.get("dryrun"), "dryrun");
        }

        for (final Map.Entry<String, Object> config : mutableParams.entrySet()) {
            final String key = config.getKey();
            if (key.equals(VERTICES) || key.equals(EDGES) || key.equals(DRYRUN)) {
                // Actions are handled elsewhere
                continue;
            }
            args.add(formatArg(KEY_TO_ARG.get(key)));
            args.add(getStringFromObject(config.getValue(), key));
        }

        // This will be local as far as spark is concerned.
        args.add(formatArg(LOCAL_MODE));

        if (dryrun) {
            args.add(formatArg(DRY_RUN));
        }

        if (vertices) {
            // If we are loading vertices, add write/verify step.
            args.add(formatArg(WRITE_VERTEX));
            args.add(formatArg(VERIFY_VERTEX));
        }

        if (edges) {
            // If we are loading edges, add write/verify steps.
            args.add(formatArg((WRITE_EDGE)));
            args.add(formatArg((VERIFY_EDGE)));
        }

        try {
            final Class<? extends FireflyBulkLoaderInterface> bulkLoaderClass = (Class<? extends FireflyBulkLoaderInterface>)
                    Class.forName("com.aerospike.firefly.bulkloader.SparkBulkLoaderMain");
            bulkLoaderClass.newInstance().load(args.toArray(new String[0]));

            // Return success if it worked, otherwise it will return an exception.
            return FireflyCloseableIteratorUtils.of((R)"Success");
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new IllegalStateException("Error, to use the bulk loader via the call API, " +
                    "use the docker image with bulk loader support.", e);
        }
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
        if (NUMBER_KEYS.contains(name)) {
            if (obj instanceof String) {
                try {
                    Double.valueOf((String) obj);
                    return (String) obj;
                } catch (final NumberFormatException ignored) {
                    throw new IllegalArgumentException("Expected bulk loader flag '" + name + "' to be set to a numeric value or numeric String value. " +
                            "Instead value was set with type '" + obj.getClass().getName() + "' which cannot be parsed to a numeric value.");
                }
            } else if (obj instanceof Number) {
                return String.valueOf(obj);
            } else {
                throw new IllegalArgumentException("Expected bulk loader flag '" + name + "' to be set to a numeric value or numeric String value. " +
                        "Instead value was set with type '" + obj.getClass().getName() + "'.");
            }
        }

        if (BOOLEAN_KEYS.contains(name)) {
            if (obj instanceof String) {
                if (!((String) obj).equalsIgnoreCase("true") && !((String) obj).equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException("Expected bulk loader flag '" + name + "' to be set to a boolean value or boolean String value. " +
                            "Instead value was set with type '" + obj.getClass().getName() + "' which cannot be parsed to a boolean value.");
                } else {
                    return (String) obj;
                }
            } else if (obj instanceof Boolean) {
                return String.valueOf(obj);
            } else {
                throw new IllegalArgumentException("Expected bulk loader flag '" + name + "' to be set to a boolean value or boolean String value. " +
                        "Instead value was set with type '" + obj.getClass().getName() + "'.");
            }
        }

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

    private static String formatArg(final String arg) {
        return "-" + arg;
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }
}
