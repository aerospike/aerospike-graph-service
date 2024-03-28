package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.bulkload.utils.FireflyBulkLoaderInterface;
import com.aerospike.firefly.structure.FireflyGraph;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_EDGES_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_ENTRY_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_DUPLICATE_VERTEX_ID_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.CONFIG_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.READ_ONLY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_WRITE_BUFFER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ENABLE_DATAFRAME_CACHING;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.GCS_EMAIL;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.GCS_KEYFILE_DIRECTORY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEY_TO_CMD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.REMOTE_PASSKEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.REMOTE_USERNAME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.SAMPLING_PERCENTAGE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERTEX_WRITE_BUFFER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VALIDATE_INPUT_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.LOCAL_MODE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERIFY_OUTPUT_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_EDGE_WRITE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_VERTEX_WRITE;

public class BulkLoaderServiceLoad<I, R> extends BulkLoaderServiceBase<I, R> {
    public static final String BULK_LOAD_SUCCESS = "Success";
    private static final String DEFAULT_CONFIG_PATH = "/opt/conf/aerospike-graph.properties";
    private static final String VERTICES = "vertices";
    private static final String EDGES = "edges";
    private static final Map<String, String> KEY_TO_ARG = new HashMap<>();
    private static final Set<String> PUBLIC_PARAMS = Set.of(
            VERTEX_DIRECTORY_KEY,
            EDGE_DIRECTORY_KEY,
            SAMPLING_PERCENTAGE,
            REMOTE_USERNAME,
            REMOTE_PASSKEY,
            GCS_EMAIL,
            GCS_KEYFILE_DIRECTORY,
            ALLOWED_DUPLICATE_VERTEX_ID_COUNT,
            ALLOWED_BAD_EDGES_COUNT,
            ALLOWED_BAD_ENTRY_COUNT
    );

    private static final Set<String> BOOLEAN_KEYS = Set.of(
            KEEP_PROVIDED_EDGE_ID_AS_PROPERTY,
            ENABLE_DATAFRAME_CACHING
    );

    private static final Set<String> NUMBER_KEYS = Set.of(
            SAMPLING_PERCENTAGE,
            VERTEX_WRITE_BUFFER,
            EDGE_WRITE_BUFFER,
            ALLOWED_DUPLICATE_VERTEX_ID_COUNT,
            ALLOWED_BAD_EDGES_COUNT,
            ALLOWED_BAD_ENTRY_COUNT
    );

    static {
        KEY_TO_ARG.put(VERTICES, null);
        KEY_TO_ARG.put(EDGES, null);
        KEY_TO_ARG.put(VALIDATE_INPUT_DATA, null);
        KEY_TO_ARG.putAll(KEY_TO_CMD);
    }

    public BulkLoaderServiceLoad(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "load";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                        "\tExpected arguments within '%s'.\n" +
                        "\tProvided argument: '%s'.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.with(\"evaluationTimeout\", 24 * 60 * 60 * 1000).call(\"%s\")\n" +
                        "\t\t\t.with(\"aerospike.graphloader.vertices\", \"/opt/aerospike-graph/etc/sampledata/vertices\")\n" +
                        "\t\t\t.with(\"aerospike.graphloader.edges\", \"/opt/aerospike-graph/etc/sampledata/edges\");\n",
                getName(), PUBLIC_PARAMS, params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        // To sanitize, dry run of parameter collection is used since the inputs are complicated and interrelated.
        try {
            // Get any provided parameters that are not allowed.
            final Sets.SetView<String> diff = Sets.difference(params.keySet(), KEY_TO_ARG.keySet());
            if (!diff.isEmpty()) {
                throw new IllegalArgumentException("The bulk loader allows the following parameters: " + PUBLIC_PARAMS + ". " +
                        "The following provided parameters are not allowed: " + diff + ".");
            }

            final List<String> args = new ArrayList<>();
            final Map<String, Object> mutableParams = new HashMap();
            mutableParams.putAll(params);
            if (!mutableParams.containsKey(CONFIG_DIRECTORY_KEY)) {
                mutableParams.put(CONFIG_DIRECTORY_KEY, DEFAULT_CONFIG_PATH);
            }
            boolean vertices = true;
            boolean edges = true;

            // The way specifying vertices or edges is that:
            // If you specify neither, both are loaded.
            // If you specify both as true, then both element types are loaded.
            // If you specify both as false, an error is returned.
            // If you specify 1 as true, that is the only element type loaded.
            // If you specify 1 as false, then only the other element type is loaded.
            if (mutableParams.containsKey(VERTICES) && mutableParams.containsKey(EDGES)) {
                vertices = getBooleanFromObject(mutableParams.get(VERTICES), VERTICES);
                edges = getBooleanFromObject(mutableParams.get(EDGES), EDGES);
            } else if (mutableParams.containsKey(VERTICES)) {
                vertices = getBooleanFromObject(mutableParams.get(VERTICES), VERTICES);
                edges = !vertices;
            } else if (mutableParams.containsKey(EDGES)) {
                edges = getBooleanFromObject(mutableParams.get(EDGES), EDGES);
                vertices = !edges;
            }

            if (!vertices && !edges) {
                throw new IllegalArgumentException("Either '" + VERTICES + "' or '" + EDGES + "' must be set to true.");
            }

            if (mutableParams.containsKey(VALIDATE_INPUT_DATA)) {
                getBooleanFromObject(mutableParams.get(VALIDATE_INPUT_DATA), VALIDATE_INPUT_DATA);
            }

            for (final Map.Entry<String, Object> config : mutableParams.entrySet()) {
                final String key = config.getKey();
                if (key.equals(VERTICES) || key.equals(EDGES) || key.equals(VALIDATE_INPUT_DATA)) {
                    // Actions are handled elsewhere
                    continue;
                }
                args.add(formatArg(KEY_TO_ARG.get(key)));
                args.add(getStringFromObject(config.getValue(), key));
            }
        } catch (final IllegalArgumentException e) {
            return false;
        }
        return true;
    }

    @Override
    protected R execute(final Map params) {
        // Get any provided parameters that are not allowed.
        final Sets.SetView<String> diff = Sets.difference(params.keySet(), KEY_TO_ARG.keySet());
        if (!diff.isEmpty()) {
            throw new IllegalArgumentException("The bulk loader allows the following parameters: " + PUBLIC_PARAMS + ". " +
                    "The following provided parameters are not allowed: " + diff + ".");
        }

        final List<String> args = new ArrayList<>();
        final Map<String, Object> mutableParams = new HashMap();
        mutableParams.putAll(params);
        if (!mutableParams.containsKey(CONFIG_DIRECTORY_KEY)) {
            mutableParams.put(CONFIG_DIRECTORY_KEY, DEFAULT_CONFIG_PATH);
        }
        boolean vertices = true;
        boolean edges = true;
        boolean validateInputData = true;

        // The way specifying vertices or edges is that:
        // If you specify neither, both are loaded.
        // If you specify both as true, then both element types are loaded.
        // If you specify both as false, an error is returned.
        // If you specify 1 as true, that is the only element type loaded.
        // If you specify 1 as false, then only the other element type is loaded.
        if (mutableParams.containsKey(VERTICES) && mutableParams.containsKey(EDGES)) {
            vertices = getBooleanFromObject(mutableParams.get(VERTICES), VERTICES);
            edges = getBooleanFromObject(mutableParams.get(EDGES), EDGES);
        } else if (mutableParams.containsKey(VERTICES)) {
            vertices = getBooleanFromObject(mutableParams.get(VERTICES), VERTICES);
            edges = !vertices;
        } else if (mutableParams.containsKey(EDGES)) {
            edges = getBooleanFromObject(mutableParams.get(EDGES), EDGES);
            vertices = !edges;
        }

        if (!vertices && !edges) {
            throw new IllegalArgumentException("Either '" + VERTICES + "' or '" + EDGES + "' must be set to true.");
        }

        if (mutableParams.containsKey(VALIDATE_INPUT_DATA)) {
            validateInputData = getBooleanFromObject(mutableParams.get(VALIDATE_INPUT_DATA), VALIDATE_INPUT_DATA);
        }

        for (final Map.Entry<String, Object> config : mutableParams.entrySet()) {
            final String key = config.getKey();
            if (key.equals(VERTICES) || key.equals(EDGES) || key.equals(VALIDATE_INPUT_DATA)) {
                // Actions are handled elsewhere
                continue;
            }
            args.add(formatArg(KEY_TO_ARG.get(key)));
            args.add(getStringFromObject(config.getValue(), key));
        }

        // This will be local as far as spark is concerned.
        args.add(formatArg(LOCAL_MODE));
        // Local mode has no Spark node workers that can fail individually to cause phantom edges.
        args.add(formatArg(READ_ONLY));

        if (validateInputData) {
            args.add(formatArg(VALIDATE_INPUT_DATA));
        }

        // These won't be simultaneously false due to check above.
        if (!vertices) {
            // If we are not loading vertices, disable step.
            args.add(formatArg(DISABLE_VERTEX_WRITE));
        }
        if (!edges) {
            // If we are not loading edges, disable step.
            args.add(formatArg((DISABLE_EDGE_WRITE)));
        }
        args.add(formatArg((VERIFY_OUTPUT_DATA)));

        try {
            final Class<? extends FireflyBulkLoaderInterface> bulkLoaderClass = (Class<? extends FireflyBulkLoaderInterface>)
                    Class.forName("com.aerospike.firefly.bulkloader.SparkBulkLoaderMain");
            bulkLoaderClass.newInstance().load(args.toArray(new String[0]));

            final String output = formatErrorCount(graph);
            return (R) output;
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

    public static String formatErrorCount(final FireflyGraph graph) {
        final AerospikeConnection db = graph.getBaseGraph();
        final long badEntryCount = db.incrementAndGetBadEntryCount(0);
        final long duplicateVertexIdCount = db.incrementAndGetDuplicateVertexIdCount(0);
        final long badEdgeCount = db.incrementAndGetBadEdgeCount(0);
        if (badEntryCount == 0 && duplicateVertexIdCount == 0 && badEdgeCount == 0) {
            return BULK_LOAD_SUCCESS;
        } else {
            final String sb = "Warning: Errors were encountered during bulk loading." +
                    "\nduplicate-vertex-id-count: " + duplicateVertexIdCount +
                    "\nbad-edge-count: " + badEdgeCount +
                    "\nbad-entry-count: " + badEntryCount +
                    // TODO: Fix below.
                    "\nUse the g.call(\""aerospike.graphloader.bulk-load.errors"\") command for details.";
            return sb;
        }
    }

    @Override
    public Map<String, String> describeParams() {
        return Map.of("See bulk loading documentation", "https://aerospike.com/docs/graph/data-loading/standalone#configuration-options");
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info(getName() + " bulk load graph.");
    }
}
