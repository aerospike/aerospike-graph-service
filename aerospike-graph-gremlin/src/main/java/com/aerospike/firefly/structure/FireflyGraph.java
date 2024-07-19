package com.aerospike.firefly.structure;

import ch.qos.logback.classic.Level;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.AerospikeLogger;
import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.io.aerospike.query.ReadInfo;
import com.aerospike.firefly.jsr223.FireflyGremlinPlugin;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException;
import com.aerospike.firefly.process.computer.local.LocalGraphComputer;
import com.aerospike.firefly.process.computer.local.LocalGraphComputerView;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyContentionHandlingStrategy;
import com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException;
import com.aerospike.firefly.runtime.exceptions.ElementNotFoundException;
import com.aerospike.firefly.runtime.exceptions.VertexRecordSizeExceededException;
import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.runtime.tasks.FireflyMetadataTask;
import com.aerospike.firefly.runtime.tasks.FireflyUsageStats;
import com.aerospike.firefly.structure.id.BufferedNumericIdManager;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.structure.id.IdManager;
import com.aerospike.firefly.structure.id.RecyclingBufferedNumericIdManager;
import com.aerospike.firefly.structure.iterator.FireflyBatchElementIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.util.FireflyTtlHandler;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.GraphFactory;
import com.aerospike.firefly.util.LoggerUtil;
import com.aerospike.firefly.util.PluginUtil;
import com.aerospike.firefly.util.WarmupUtil;
import com.aerospike.firefly.util.concurrency.FireflyRecordLockHandler;
import org.apache.commons.configuration2.Configuration;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_DATA_SIZE;
import static com.aerospike.firefly.structure.FireflyEdge.IN_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.LABEL_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.OUT_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.PROPERTIES_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.TYPE_HINTS_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.createFilterableSupernodeOperations;
import static com.aerospike.firefly.structure.FireflyGraphSummaryVertex.GRAPH_SUMMARY_VERTEX;
import static com.aerospike.firefly.structure.FireflyVertex.SUPERNODE_PROPERTY_KEY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.BULK_LOADER_FLAG;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.BULK_LOAD_ID_BUFFER_SIZE;
import static com.aerospike.firefly.util.Tokens.EDGE_RECYCLED_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.EDGE_UNIQUE_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.UNIMPLEMENTED;
import static com.aerospike.firefly.util.Tokens.VERTEX_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.VERTEX_PROPERTY_ID_COUNTER;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn("com.aerospike.firefly.structure.process.CustomGraphProcessStandardTest")
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)

// GraphComputer OptOuts
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "*", reason = "REMOVE -- currently here for faster testing", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$GreedyMatchTraversals", method = "*", reason = "REMOVE -- currently here for faster testing", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$CountMatchTraversals", method = "*", reason = "REMOVE -- currently here for faster testing", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConnectedComponentTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PageRankTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PeerPressureTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProgramTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ShortestPathTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgramTest", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgramTest", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgramTest", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest$Traversals", method = "*", reason = "Firefly does not support arbitrary object starts", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest", method = "*", reason = "Firefly does not support thread interruption ?? why not ??", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})


// Tests that require lambda support.
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV1Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV2Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV3Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoV1Test", method = "shouldSerializePathAsDetached", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoV3Test", method = "shouldSerializePathAsDetached", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.TransactionTest", method = "*", reason = "MAKE ACTIVE WHEN TRANSACTIONS IMPLEMENTED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest", method = "*", reason = "MAKE ACTIVE WHEN PARALLEL SCAN RESULT ITERATOR IMPLEMENTED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest", method = "*", reason = "THROW PROPER EXCEPTIONS WHEN DESIRED FINAL FEATURE SET IS DETERMINED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "*", reason = "THESE TESTS READ AND WRITE FROM 2 GRAPHS, BUT WHEN BACKED BY THE SAME AEROSPIKE INSTANCE, PRODUCE INVALID RESULTS", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest", method = "*", reason = "CURRENTLY DO NOT WORK, NEED TO FIX AND ENABLE", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldEvaluateConnectivityPatterns", reason = "This test fails due to caching.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.VertexPropertyTest$VertexPropertyRemoval", method = "shouldRemoveMultiPropertiesWhenVerticesAreRemoved", reason = "Replaced in TestAerospikeGraphIntegration with cache-friendly implementation.", computers = {"ALL"})

// Firefly does not support Float ids
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentation", reason = "Firefly does not support Float ids", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentations", reason = "Firefly does not support Float ids", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentations", reason = "Firefly does not support Float ids", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentation", reason = "Firefly does not support Float ids", computers = {"ALL"})

public class FireflyGraph implements Graph, WrappedGraph<AerospikeConnection> {
    public static final String FIREFLY_CONFIGURATION_VARIABLE_NAME = "FIREFLY_CONFIGURATION";
    public static final String FIREFLY_WARMUP_VARIABLE_NAME = "FIREFLY_WARMUP";
    public static final String DATA_MODEL = "packed";

    // AerospikeGraphService is a dummy class that allows us to instantiate a logger in FireflyGraph that says
    // AerospikeGraphService. We can eventually migrate to calling FireflyGraph AerospikeGraphService but this requires
    // docs changes, config updates, etc, and isn't worth it right now.
    public static final String PRODUCT_NAME = "Aerospike Graph";
    private static final Logger LOG = LoggerFactory.getLogger(PRODUCT_NAME);

    public static String FIREFLY_VERSION = "2.3.0-SNAPSHOT";
    public final AtomicBoolean closed = new AtomicBoolean(false);
    private final Timer fireflyCardinalityMetadataTask = new Timer(true);
    private final Timer fireflyIndexMetadataTask = new Timer(true);
    private final FireflyGraphFeatures features;
    private final Configuration configuration;
    public static String VP_INDEX_PREFIX = "VP";
    public static String EP_INDEX_PREFIX = "EP";
    private final FireflyGraphVariables variables;
    protected final AerospikeConnection db;
    private final FireflyIdFactory idFactory;
    public LocalGraphComputerView graphComputerView = null;
    public final boolean bulkLoaderFlag;
    public final long bulkLoadIdBufferSize;
    public final IdManager<Long> vertexIdManager;
    public final IdManager<byte[]> edgeIdManager;
    public final IdManager<Long> vertexPropertyIdManager;
    private final FireflyTtlHandler ttlHandler;
    public final FireflyCardinalityMetadata fireflyCardinalityMetadata;
    public final FireflyIndexMetadata fireflyIndexMetadata;
    public final FireflyGraphSummaryUpdater fireflySummaryUpdater;
    private final FireflyRecordLockHandler fireflyRecordLockHandler;
    private final ServiceRegistry serviceRegistry = new ServiceRegistry();
    private static final String GREMLIN_SERVER_YAML_PATH = "GREMLIN_SERVER_YAML_PATH";
    private static final String UNIFIED_CONFIG_PROPERTIES_PATH = "UNIFIED_CONFIG_PROPERTIES_PATH";
    private final Settings gremlinServerSettings;
    private static final AtomicBoolean INFO_PRINTED = new AtomicBoolean(false);

    static {
        synchronized (TraversalStrategies.GlobalCache.class) {
            TraversalStrategies.GlobalCache.registerStrategies(
                    FireflyGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                            .addStrategies(new FireflyContentionHandlingStrategy())
                            .addStrategies(OptionsStrategy.build().create()));
        }
    }

    public FireflyGraph(final AerospikeConnection db, final Configuration conf, final Settings gremlinServerSettings) {
        this.gremlinServerSettings = gremlinServerSettings;
        this.configuration = conf;
        db.createGraphIndexes();
        this.db = db;
        this.idFactory = db.getIdFactory();
        this.bulkLoaderFlag = ConfigurationHelper.getOrDefaultBool(BULK_LOADER_FLAG, conf);
        this.bulkLoadIdBufferSize = ConfigurationHelper.getOrDefaultInt(BULK_LOAD_ID_BUFFER_SIZE, conf);

        this.vertexPropertyIdManager = new BufferedNumericIdManager(VERTEX_PROPERTY_ID_COUNTER, db.PROPERTY_ID_BUFFER_SIZE, false);
        this.vertexIdManager = new BufferedNumericIdManager(VERTEX_ID_COUNTER, db.VERTEX_ID_BUFFER_SIZE, true);
        this.edgeIdManager = new RecyclingBufferedNumericIdManager(EDGE_RECYCLED_ID_COUNTER, EDGE_UNIQUE_ID_COUNTER, db.EDGE_ID_BUFFER_SIZE, false);
        this.variables = new FireflyGraphVariables(this);
        this.features = new FireflyGraphFeatures(this);

        // Create index metadata background task that will populate indexes for the named graph on the fly.
        fireflyIndexMetadata = new FireflyIndexMetadata(db);
        final TimerTask indexMetadataTimerTask = new FireflyMetadataTask(fireflyIndexMetadata);
        fireflyIndexMetadataTask.schedule(indexMetadataTimerTask, 0, db.INDEX_METADATA_UPDATE_FREQUENCY);

        // Grab user defined vertex property indexes from the configuration and create them.
        final List<String> vertexPropertyIndexes = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES, configuration);
        createIndexes(FireflyVertex.class, db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN, db.getVpIndexPrefix(), vertexPropertyIndexes);

        // Grab user defined edge property indexes from the configuration and create them.
        final List<String> edgePropertyIndexes = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.EDGE_PROPERTY_INDEXES, configuration);
        if (edgePropertyIndexes != null && !edgePropertyIndexes.isEmpty()) {
            // TODO: Edge indexes.
            throw new RuntimeException("Edge property indexes are not currently supported.");
        }
        createIndexes(FireflyEdge.class, db.PROPERTIES_BIN, db.getEpIndexPrefix(), edgePropertyIndexes);

        // Create ttl background task.
        this.ttlHandler = new FireflyTtlHandler(this);

        // Create cardinality metadata background task that will populate cardinality for the named graph on the fly.
        fireflyCardinalityMetadata = new FireflyCardinalityMetadata(db, db.V_LABEL_INDEX_NAME, db.E_LABEL_INDEX_NAME, fireflyIndexMetadata);
        final TimerTask cardinalityMetadataTimerTask = new FireflyMetadataTask(fireflyCardinalityMetadata);

        fireflyCardinalityMetadataTask.schedule(cardinalityMetadataTimerTask, 0, db.CARDINALITY_METADATA_UPDATE_FREQUENCY);
        fireflySummaryUpdater = new FireflyGraphSummaryUpdater(db);
        fireflyRecordLockHandler = new FireflyRecordLockHandler(db);

        if (conf.containsKey(ConfigurationHelper.Keys.PLUGIN)) {
            final String pluginConfigString = conf.getString(ConfigurationHelper.Keys.PLUGIN);
            final List<String> plugins = Arrays.asList(pluginConfigString.split(","));
            for (final String plugin : plugins) {
                PluginUtil.loadPlugin(plugin, conf, this);
            }
        }

        if (!db.WARMUP_MODE) {
            // Create usage statistics background task.
            FireflyUsageStats.startUsageStats(db);

            // Start metrics.
            FireflyGremlinPlugin.initializeGraphMetrics(this);

            // Register admin services graph metrics since it will bootstrap the server.
            AdminServiceRegistry.registerAdminServices(this);
        }
    }

    public static FireflyGraph open(final Configuration conf) {
        ConfigurationHelper.validateConfig(conf);
        final String logLevel = (System.getenv("FIREFLY_TESTING") != null &&
                System.getenv("FIREFLY_TESTING").equalsIgnoreCase("true")) ?
                "WARN" : ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.LOG_LEVEL, conf);
        final boolean clientLogging = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ASCLIENT_LOG_ENABLED, conf);
        final boolean preheat = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUTO_PRE_HEAT, conf);
        try {
            // Prevent warmup from disabling the logger for Aerospike Client.
            if (clientLogging && !ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.WARMUP_MODE, conf)) {
                Log.setCallback(new AerospikeLogger());
                Log.setLevel(Log.Level.valueOf(logLevel));
            }
            LoggerUtil.setLogLevel(Level.toLevel(logLevel));
        } catch (final Exception e) {
            LOG.warn("Failed to set log level {}", e.getMessage());
        }
        try {
            // FIREFLY_TESTING is set strictly by surefire plugin so not in any customer systems.
            // This makes our testing logs 100x smaller.
            boolean infoPrinted = INFO_PRINTED.get();
            if (System.getenv("FIREFLY_TESTING") == null ||
                    !System.getenv("FIREFLY_TESTING").equalsIgnoreCase("true")
                            && !infoPrinted) {
                final Runtime javaRuntime = Runtime.getRuntime();
                LOG.info("Java Runtime: {} available processors.", javaRuntime.availableProcessors());
                LOG.info("Java Runtime: {} MB max memory.", javaRuntime.maxMemory() / (1024 * 1024));
                LOG.info("Java Runtime: {} MB total memory.", javaRuntime.totalMemory() / (1024 * 1024));
                LOG.info("Java Runtime: {} MB free memory.", javaRuntime.freeMemory() / (1024 * 1024));
                LOG.info("JVM Vendor: {}", System.getProperty("java.vm.vendor"));
                LOG.info("JVM Specification Vendor: {}.", System.getProperty("java.vm.specification.vendor"));
                LOG.info("Java Specification Version: {}.", System.getProperty("java.specification.version"));
                LOG.info("JVM Runtime: {}.", System.getProperty("java.runtime.name"));
                LOG.info("JVM Runtime Version: {}.", System.getProperty("java.runtime.version"));

                // Straight up printing out conf just provides a class name / memory address.
                final Iterator<String> keys = conf.getKeys();
                final Map<String, Object> configurationMap = new HashMap<>();
                while (keys.hasNext()) {
                    final String key = keys.next();
                    if (!key.contains("password") && !key.contains("secret") && !key.contains("token")) {
                        configurationMap.put(key, conf.getProperty(key));
                    } else {
                        configurationMap.put(key, "********");
                    }
                }
                LOG.info("Aerospike Graph Service configuration: {}.", configurationMap);
            }
            LOG.info("Starting Aerospike Graph Service v{}.", FIREFLY_VERSION.replace("-SNAPSHOT", ""));

            INFO_PRINTED.set(true);
            if (ConfigurationHelper.getOrDefaultBool(BULK_LOADER_FLAG, conf)) {
                // If we are in bulk load mode, sleep between 0 and 1 second to allow Aerospike time between spark
                // works initializing.
                final Random random = new Random();
                try {
                    Thread.sleep(random.nextInt(5000));
                } catch (final InterruptedException ignored) {
                    // Propagate the interrupt but ignore it for the context of the sleep.
                    Thread.currentThread().interrupt();
                }
            }

            if (preheat)
                WarmupUtil.create(conf).preheat(WarmupUtil.passes);
            return GraphFactory.createGraph(AerospikeConnection.connect(conf), conf);
        } catch (final Exception e) {
            LOG.error("=================== FAILED TO START AEROSPIKE GRAPH SERVICE ===================");
            LOG.error("========== Aerospike Graph Service failing to start is usually a result of an incorrect configuration.");
            LOG.error("========== Verify that the Aerospike IP and port are correct.");
            LOG.error("========== See Error message for more details:", e);

            // Signal to gremlin-server to shut down.
            System.exit(1);

            // Required to compile.
            return null;
        }
    }

    public static final String GETDATAMODELNAME = "getDataModelName";
    public static final String DATAMODELVERSION = "dataModelVersion";

    public static ComparableVersion dataModelVersion() {
        return new ComparableVersion(FIREFLY_VERSION);
    }

    private static Settings GREMLIN_SERVER_SETTINGS = null;

    public static synchronized String getGremlinServerYamlFile() {
        final String gremlinServerYamlPath = System.getenv(GREMLIN_SERVER_YAML_PATH);
        if ((gremlinServerYamlPath == null || gremlinServerYamlPath.isEmpty()) ||
                !(new File(gremlinServerYamlPath).exists())) {
            throw new RuntimeException("Failed to load docker settings file, " +
                    "must be running docker image to read the gremlin-server yaml.");
        }
        return gremlinServerYamlPath;
    }

    public static synchronized String getUnifiedConfigFile() {
        final String unifiedConfigPath = System.getenv(UNIFIED_CONFIG_PROPERTIES_PATH);
        if ((unifiedConfigPath == null || unifiedConfigPath.isEmpty()) ||
                !(new File(unifiedConfigPath).exists())) {
            throw new RuntimeException("Failed to load docker settings file, " +
                    "must be running docker image to read the gremlin-server yaml.");
        }
        return unifiedConfigPath;
    }

    public static synchronized Settings getGremlinServerSettings() {
        if (GREMLIN_SERVER_SETTINGS == null) {
            try {
                final String yamlLocation = getGremlinServerYamlFile();
                GREMLIN_SERVER_SETTINGS = Settings.read(yamlLocation);
            } catch (final Exception e) {
                if (System.getenv("FIREFLY_TESTING") == null ||
                        !System.getenv("FIREFLY_TESTING").equalsIgnoreCase("true")) {
                    LOG.error("Failed to load gremlin-server settings file.", e);
                }
                GREMLIN_SERVER_SETTINGS = new Settings();
            }
        }
        return GREMLIN_SERVER_SETTINGS;
    }

    /**
     * Return the FireflyIdFactory
     *
     * @return FireflyIdFactory
     */
    public FireflyIdFactory getIdFactory() {
        return idFactory;
    }

    public String getDataModel() {
        return FireflyGraph.getDataModelName();
    }

    protected int getTypeHint() {
        return FireflyVertex.VERTEX_TYPE_HINT;
    }

    /**
     * Function to write vertex to Aerospike.
     *
     * @param idValue    Id of vertex.
     * @param label      Label of vertex.
     * @param properties List of vertex properties.
     * @return Vertex.
     */
    public FireflyVertex writeVertex(final FireflyId idValue,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties) {
        // If the supernode property flag is set on the vertex write, remove it from the write steam and assign it.
        final Map.Entry supernodeFlag = properties.stream().filter(e -> e.getKey().equals(SUPERNODE_PROPERTY_KEY)).findFirst().orElse(null);
        if (supernodeFlag != null) {
            properties.remove(supernodeFlag);
        }
        final boolean isEdgeCacheOverflowed = !this.db.GLOBAL_EDGE_CACHE_ENABLED_FLAG ||
                this.db.ON_RECORD_ID_LIMIT <= 0 || supernodeFlag != null;
        return FireflyVertex.writeVertex(this, idValue, label, properties, getTypeHint(), true, isEdgeCacheOverflowed);
    }

    public void mergeVertex(final Object id, final String label, final List<Map.Entry<String, Object>> properties) {
        int tryCount = 0;
        while (true) {
            try {
                final Map<Object, Object> propertiesMatch = new HashMap<>();
                properties.forEach(entry -> propertiesMatch.put(entry.getKey(), entry.getValue()));
                final Map<Object, Object> propertiesCreate = new HashMap<>();
                properties.forEach(entry -> propertiesCreate.put(entry.getKey(), entry.getValue()));
                propertiesCreate.put(T.id, id);
                propertiesCreate.put(T.label, label);
                propertiesMatch.remove(T.id);
                propertiesMatch.remove(T.label);
                traversal().mergeV(CollectionUtil.asMap(T.id, id))
                        .option(Merge.onMatch, propertiesMatch)
                        .option(Merge.onCreate, propertiesCreate).iterate();
                break;
            } catch (final IllegalArgumentException e) {
                if (!e.getMessage().contains("Vertex with id already exists")) {
                    throw e;
                }
                // A mergeE can fail due to vertex with id already existing b/c it is not a true transaction and we may
                // be inserting many updates to the vertex. However once the vertex does exist this should not fail a second time.
                // Testing has shown that if this happens it tends to happen a lot so it is best left not logged and let the error come out later if
                // the implementation is wrong.
                if (tryCount > 0) {
                    throw e;
                }
                tryCount++;
            } catch (final VertexRecordSizeExceededException vrsee) {
                throw new FireflyLoadingException((AerospikeException) vrsee.getCause(), vrsee.getMessage());
            } catch (final AerospikeException ae) {
                throw new FireflyLoadingException(ae);
            }
        }
    }

    public void bulkWriteVertex(final FireflyId idValue,
                                final String label,
                                final List<Map.Entry<String, Object>> properties,
                                final boolean supernode) {
        try {
            // We do not use ~supernode flag to allow forcing a vertex to a supernode when bulk loading since it impacts our
            // bulk loader flow and also we already have to check for this regardless inside the bulk loader.
            FireflyVertex.writeVertex(this, idValue, label, properties, getTypeHint(), false, supernode);
        } catch (final VertexRecordSizeExceededException vrsee) {
            throw new FireflyLoadingException((AerospikeException) vrsee.getCause(), vrsee.getMessage());
        } catch (final AerospikeException ae) {
            throw new FireflyLoadingException(ae);
        }
    }

    public void writeDuplicateVertexId(final Object vertexId, final long count) {
        final FireflyId id = getIdFactory().createFromUser(FireflyVertex.class, vertexId);
        final Key key = new Key(db.namespace, db.BULK_LOAD_DUPLICATE_VID_SET, Value.get(id.getStorageId()));
        final Bin addBin = new Bin(db.COUNTER_BIN, count);
        final WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        policy.sendKey = true;
        try {
            this.db.writeOperate(policy, key, Operation.add(addBin));
        } catch (final AerospikeException e) {
            // Do not retry this or fail because this list being slightly incorrect is inconsequential and will reduce
            // bulk load speed
            LOG.warn("Error recording duplicate Vertex ID details ID: " + vertexId);
        }
    }

    public Iterator<Map<String, Object>> readDuplicateVertexIdErrors() {
        return GraphQuery.create(this).scanSet(null, db.BULK_LOAD_DUPLICATE_VID_SET, null, null, keyRecord -> {
            final Map<String, Object> errorInfo = new HashMap<>();
            errorInfo.put("id", keyRecord.key.userKey.getObject());
            errorInfo.put("count", keyRecord.record.getLong(db.COUNTER_BIN));
            return errorInfo;
        });
    }

    public void writeBadEntry(final String row, final String fileName) {
        final Key key = new Key(db.namespace, db.BULK_LOAD_BAD_ENTRY_SET, Value.get(UUID.randomUUID().toString()));
        final Bin rowBin = new Bin(db.BL_ROW_BIN, row);
        final Bin fileBin = new Bin(db.BL_FILE_BIN, fileName);
        final WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        policy.sendKey = false;
        try {
            this.db.writeOperate(policy, key, Operation.put(rowBin), Operation.put(fileBin));
        } catch (final AerospikeException e) {
            // Do not retry this or fail because this list being slightly incorrect is inconsequential and will reduce
            // bulk load speed
            LOG.warn("Error recording bad entry row \"" + row + "\" in file \"" + fileName + "\"");
        }
    }

    public Iterator<Map<String, String>> readBadEntryErrors() {
        return GraphQuery.create(this).scanSet(null, db.BULK_LOAD_BAD_ENTRY_SET, null, null, keyRecord -> {
            final Map<String, String> errorInfo = new HashMap<>();
            errorInfo.put("row", keyRecord.record.getString(db.BL_ROW_BIN));
            errorInfo.put("file", keyRecord.record.getString(db.BL_FILE_BIN));
            return errorInfo;
        });
    }

    public void writeBadEdge(final Object badVertexId, final long count) {
        final FireflyId id = getIdFactory().createFromUser(FireflyVertex.class, badVertexId);
        final Key key = new Key(db.namespace, db.BULK_LOAD_BAD_EDGE_SET, Value.get(id.getStorageId()));
        final Bin addBin = new Bin(db.COUNTER_BIN, count);
        final WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        policy.sendKey = true;
        try {
            this.db.writeOperate(policy, key, Operation.add(addBin));
        } catch (final AerospikeException e) {
            // Do not retry this or fail because this list being slightly incorrect is inconsequential and will reduce
            // bulk load speed
            LOG.warn("Error recording bad Edge data with failed Vertex ID: " + badVertexId);
        }
    }

    public Iterator<Map<String, Object>> readBadEdgeErrors() {
        return GraphQuery.create(this).scanSet(null, db.BULK_LOAD_BAD_EDGE_SET, null, null, keyRecord -> {
            final Map<String, Object> errorInfo = new HashMap<>();
            errorInfo.put("bad-vertex-id", keyRecord.key.userKey.getObject());
            errorInfo.put("count", keyRecord.record.getLong(db.COUNTER_BIN));
            return errorInfo;
        });
    }

    /**
     * Function to bulk write edges to a vertex's edge cache
     *
     * @param vertexId  Vertex label.
     * @param direction Direction of the edges.
     * @param edgeIds   List of edge IDs.
     * @param edgeLabel Label of all edges in edge ID list.
     */
    public void bulkWriteEdgesToVertexCache(final FireflyId vertexId, final Direction direction,
                                            final List<Value> edgeIds, final String edgeLabel) {
        // Get the key.
        final Key key = FireflyRecord.getKey(db, this.db.VERTEX_AERO_SET, vertexId);

        // Get direction and counter keys. Direction must be IN or OUT.
        final String directionBinName = direction == Direction.IN ? this.db.IN_EDGES_BIN : this.db.OUT_EDGES_BIN;

        // Create the operations.
        final ListPolicy preventDuplicates = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);
        final Operation appendEdgeId = ListOperation.appendItems(
                preventDuplicates,
                directionBinName,
                edgeIds,
                CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)
        );

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            this.db.writeOperate(writePolicy, key, appendEdgeId);
        } catch (final ElementNotFoundException enfe) {
            throw new FireflyLoadingException((AerospikeException) enfe.getCause());
        } catch (final VertexRecordSizeExceededException vrsee) {
            throw new FireflyLoadingException((AerospikeException) vrsee.getCause(), vrsee.getMessage());
        } catch (final AerospikeException ae) {
            throw new FireflyLoadingException(ae);
        }
    }

    /**
     * Function to read vertex from Aerospike.
     *
     * @param idValue Id of vertex.
     * @return Vertex.
     */
    public FireflyVertex readVertex(final FireflyId idValue) {
        final List<FireflyVertex> vertices = readVertices(List.of(), List.of(idValue), null);
        if (vertices.isEmpty()) {
            return null;
        } else {
            return vertices.get(0);
        }
    }

    public List<FireflyVertex> readVertices(final List<HasContainer> hasContainers,
                                            final List<FireflyId> idValues,
                                            final List<String> requiredProperties) {
        final ReadInfo readInfo = ReadInfo.create().
                set(db.VERTEX_AERO_SET).
                ids(idValues).
                reqProps(requiredProperties).
                exp(hasContainers, db, FireflyVertex.class).
                build();
        return FireflyVertex.readVertices(this, readInfo);
    }

    /**
     * Function to create vertex from a KeyRecord.
     *
     * @param keyRecord KeyRecord to use.
     * @return Vertex.
     */
    public FireflyVertex vertexFromRecord(final KeyRecord keyRecord) {
        return FireflyVertex.fromRecord(this, keyRecord);
    }

    public FireflyId vertexIdFromRecord(final KeyRecord keyRecord) {
        return getIdFactory().createId(keyRecord.key.userKey.getObject(), FireflyVertex.class);
    }

    public FireflyId edgeIdFromRecord(final KeyRecord keyRecord) {
        return getIdFactory().createId(keyRecord.key.userKey.getObject(), FireflyVertex.class);
    }

    // This function is used via reflection in Upgrade.java. Removing will cause issues.
    public static String getDataModelName() {
        return DATA_MODEL;
    }

    /**
     * Function to write edge to Aerospike.
     *
     * @param edgeId     Edge id.
     * @param label      Edge label.
     * @param properties Edge properties.
     * @param inVertex   In vertex of edge.
     * @param outVertex  Out vertex of edge.
     * @return Edge.
     */
    public FireflyEdge writeEdge(final FireflyId edgeId,
                                 final String label,
                                 final List<Map.Entry<String, Object>> properties,
                                 final FireflyVertex inVertex,
                                 final FireflyVertex outVertex) {
        // Write edge to vertex, if edge write fails, null check on edge record will protect from inconsistent data.
        // Add edge to inVertex and outVertex.
        final boolean inVertexCacheWrite = inVertex.writeEdge(Direction.IN, getIdFactory().createCompositeEdgeId(edgeId, outVertex.id), label);
        final boolean outVertexCacheWrite = outVertex.writeEdge(Direction.OUT, getIdFactory().createCompositeEdgeId(edgeId, inVertex.id), label);

        // Write edge to Aerospike and return FireflyEdge.
        return FireflyEdge.writeEdge(this, edgeId, label, properties, inVertex, outVertex, inVertexCacheWrite, outVertexCacheWrite);
    }

    public void bulkWriteEdge(final byte[] edgeId, final String label, final List<Map.Entry<String, Object>> properties,
                              final Object inVertexId, final Object outVertexId, final boolean inVSupernode,
                              final boolean outVSupernode) {
        FireflyGraph.LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeId, outVertexId, label, inVertexId, properties);
        final FireflyId id = getIdFactory().createId(edgeId, FireflyEdge.class);
        final FireflyId inId = FireflyIdPoly.fromObject(inVertexId, db.VERTEX_AERO_SET);
        final FireflyId outId = FireflyIdPoly.fromObject(outVertexId, db.VERTEX_AERO_SET);

        final Map<String, Object> propertyMap = new TreeMap<>();
        final Map<String, Object> typeHints = new TreeMap<>();
        properties.forEach(property -> {
            final String key = property.getKey();
            final Object value = property.getValue();
            FireflyHelper.validatePropertyValue(value);

            if (value == null) {
                propertyMap.remove(key);
                typeHints.remove(key);
            } else {
                propertyMap.put(key, value);
                final Object typeHint = getTypeHintOf(value);
                if (typeHint != null) {
                    typeHints.put(key, typeHint);
                }
            }
        });

        final List<Value> edgeData = new ArrayList<>(EDGE_DATA_SIZE);

        final List<Operation> operations = new ArrayList<>();
        // CREATE and UPDATE are both okay since this is idempotent.
        final MapPolicy edgeMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);

        // Add label to Edge data.
        edgeData.add(LABEL_POSITION, Value.get(label));
        // Add IN and OUT to Edge data.
        edgeData.add(IN_V_POSITION, Value.get(inId.getKeyHashString()));
        edgeData.add(OUT_V_POSITION, Value.get(outId.getKeyHashString()));

        // Write to supernodes bin if vertex cache overflowed.
        if (inVSupernode) {
            final Operation writeInVSupernode = MapOperation.put(edgeMapPolicy, db.SUPERNODES_IN_BIN,
                    Value.get(edgeId), Value.get(inId.getKeyHashString()));
            operations.add(writeInVSupernode);
        }
        if (outVSupernode) {
            final Operation writeOutVSupernode = MapOperation.put(edgeMapPolicy, db.SUPERNODES_OUT_BIN,
                    Value.get(edgeId), Value.get(outId.getKeyHashString()));
            operations.add(writeOutVSupernode);
        }

        // Write to filterable supernode bin if necessary.
        operations.addAll(createFilterableSupernodeOperations(this, (FireflyPhatEdgeId) id, outVSupernode,
                inVSupernode, outId, inId, label, propertyMap));

        // Add properties and type hints to Edge data.
        edgeData.add(PROPERTIES_POSITION, Value.get(propertyMap));
        edgeData.add(TYPE_HINTS_POSITION, Value.get(typeHints));

        // Create Operation for writing Edge data.
        final Operation createIndividualEdgeMap = MapOperation.put(edgeMapPolicy, db.EDGE_DATA_BIN,
                Value.get(edgeId), Value.get(edgeData));
        operations.add(createIndividualEdgeMap);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        final Key key = getKey(db, db.EDGE_AERO_SET, id);
        try {
            db.writeOperate(writePolicy, key, operations.toArray(new Operation[0]));
        } catch (final EdgeRecordSizeExceededException ersee) {
            throw new FireflyLoadingException((AerospikeException) ersee.getCause(), ersee.getMessage());
        } catch (final AerospikeException ae) {
            throw new FireflyLoadingException(ae);
        }
        fireflySummaryUpdater.addEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
    }

    /**
     * Function to read edges from Aerospike.
     *
     * @param edgeIds Edge ids.
     * @return Edge.
     */
    public List<FireflyEdge> readEdges(final List<HasContainer> hasContainers, final List<FireflyId> edgeIds, final List<String> requiredProperties) {
        if (!hasContainers.isEmpty()) {
            throw new RuntimeException("Pushdown is not currently supported for Edges.");
        }
        if (requiredProperties != null && !requiredProperties.isEmpty()) {
            // Should never happen.
            throw new RuntimeException("Required properties are not currently supported for Edges.");
        }
        return FireflyEdge.readEdges(this, edgeIds);
    }

    /**
     * Return a Set of all the Graph variable names
     *
     * @return Set of Graph variable keys
     */
    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db,
                db.GRAPH_VARIABLES_SET,
                FireflyIdPoly.fromObject(db.GRAPH_VARIABLES_REC_KEY, db.GRAPH_VARIABLES_SET));
        if (fireflyRecord == null) return new HashSet<>();
        final Map<String, ?> m = (Map<String, ?>) fireflyRecord.record().getMap(db.GRAPH_VARIABLES_BIN);
        return m.keySet();
    }

    /**
     * Write a Graph variable
     *
     * @param key   Graph variable key
     * @param value Graph variable value to write
     * @param <V>   Graph variable value type
     */
    public <V> void writeGraphVariable(final String key, final V value) {
        db.writeTypeHintedGraphVariable(db.GRAPH_VARIABLES_SET,
                FireflyIdPoly.fromObject(db.GRAPH_VARIABLES_REC_KEY, db.GRAPH_VARIABLES_SET),
                db.GRAPH_VARIABLES_BIN,
                key,
                value,
                db.TYPE_HINTS_BIN);
    }

    /**
     * Return a Graph variable value by name
     *
     * @param key Graph variable key
     * @param <V> type
     * @return Graph variable value
     */
    public <V> V readGraphVariable(final String key) {
        if (Objects.equals(key, FIREFLY_CONFIGURATION_VARIABLE_NAME)) {
            return (V) this.configuration();
        }
        return db.readTypeHintedValueFromMap(
                db.GRAPH_VARIABLES_SET,
                FireflyIdPoly.fromObject(db.GRAPH_VARIABLES_REC_KEY, db.GRAPH_VARIABLES_SET),
                db.GRAPH_VARIABLES_BIN,
                key,
                db.TYPE_HINTS_BIN);
    }

    /**
     * Remove a Graph variable
     *
     * @param key Graph variable key to remove
     */
    public void removeGraphVariable(final String key) {
        db.removeTypeHintedValueFromMap(
                db.GRAPH_VARIABLES_SET,
                FireflyIdPoly.fromObject(db.GRAPH_VARIABLES_REC_KEY, db.GRAPH_VARIABLES_SET),
                db.GRAPH_VARIABLES_BIN,
                key,
                db.TYPE_HINTS_BIN);
    }

    /**
     * Write vertex property to Aerospike.
     *
     * @param idValue FireflyId of vertex property to write.
     * @param vertex  Vertex to write property to.
     * @param key     Key of property to write.
     * @param value   Value of property to write.
     * @param <V>     Type of value to write.
     * @return FireflyVertexProperty
     */
    public <V> FireflyVertexProperty<V> writeVertexProperty(final FireflyId idValue,
                                                            final FireflyVertex vertex,
                                                            final String key,
                                                            final V value,
                                                            final Object... keyValues) {
        final Map<String, Object> properties = new TreeMap<>();
        final Map<String, Object> typeHints = new TreeMap<>();
        final boolean allowNullProperties = features().vertex().properties().supportsNullPropertyValues();

        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!keyValues[i].equals(T.id) && !keyValues[i].equals(T.label))
                if (keyValues[i + 1] != null) {
                    properties.put((String) keyValues[i], keyValues[i + 1]);
                    final Object typeHint = getTypeHintOf(keyValues[i + 1]);
                    if (typeHint != null) {
                        typeHints.put((String) keyValues[i], typeHint);
                    }
                } else if (allowNullProperties) {
                    properties.put((String) keyValues[i], keyValues[i + 1]);
                }
            // Since this the first insertion, a null value with allowNullProperties is irrelevant, because there is no
            // properties to remove, so just ignore.
        }

        // Write vertex property to Aerospike.
        final FireflyVertexProperty<V> fireflyVertexProperty = new FireflyVertexProperty<>(
                this, idValue, vertex, key, value, properties, typeHints);

        // Append vertex property to vertex.
        vertex.writeVertexProperty(fireflyVertexProperty);

        // Return FireflyVertexProperty.
        return fireflyVertexProperty;
    }

    public long getVertexCount(final List<HasContainer> hasContainers) {
        return FireflyCloseableIteratorUtils.count(GraphQuery.create(this).scanVertexIds(hasContainers));
    }

    public long getEdgeCount() {
        return FireflyCloseableIteratorUtils.count(GraphQuery.create(this).scanEdgeIds());
    }

    @Override
    public AerospikeConnection getBaseGraph() {
        return db;
    }

    public FireflyRecordLockHandler getRecordLockHandler() {
        return fireflyRecordLockHandler;
    }

    @Override
    public Features features() {
        return features;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        // Validate key value pairs are valid for TinkerPop.
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        // Validate key value pairs are valid for Firefly.
        final Iterator i = FireflyCloseableIteratorUtils.asIterator(keyValues);
        while (i.hasNext()) {
            i.next();
            FireflyHelper.validatePropertyValue(i.next());
        }

        // If a user-supplied id is provided and it is not supported, throw exception.
        if (ElementHelper.getIdValue(keyValues).isPresent() && !features.vertex().supportsUserSuppliedIds())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        final List<Map.Entry<String, Object>> properties = convertFullyQualified(
                this.features().vertex().supportsNullPropertyValues(), keyValues);

        // Create a new id or use the provided user-supplied id (if present and supported).
        FireflyId idValue;
        if (ElementHelper.getIdValue(keyValues).isEmpty()) {
            idValue = getIdFactory().createFromManager(this, FireflyVertex.class);
            Vertex v = null;
            while (v == null) {
                try {
                    v = writeVertex(idValue, label, properties);
                } catch (final AerospikeException e) {
                    if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                        idValue = getIdFactory().createFromManager(this, FireflyVertex.class);
                    } else {
                        throw e;
                    }
                }
            }
            return v;
        } else {
            try {
                idValue = getIdFactory().createFromKeyValues(FireflyVertex.class, keyValues);
            } catch (IllegalArgumentException ignored) {
                // Invalid type for id.
                throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            }
            try {
                return writeVertex(idValue, label, properties);
            } catch (final AerospikeException e) {
                if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                    throw Graph.Exceptions.vertexWithIdAlreadyExists(idValue.getUserId());
                } else {
                    throw e;
                }
            }
        }
    }

    public List<Map.Entry<String, Object>> convertFullyQualified(final boolean supportNullProperties, final Object... propertyKeyValues) {
        List<Map.Entry<String, Object>> properties = new ArrayList<>();
        for (int i = 0; i < propertyKeyValues.length; i += 2) {
            // Skip label and user supplied id key/value pairs.
            // Also, if we do not support null and it is null, skip as well. We don't need to explicitly remove
            // null properties here since we are doing a fully qualified write and will overwrite regardless.
            if (propertyKeyValues[i].equals(T.id) || propertyKeyValues[i].equals(T.label) || (!supportNullProperties && propertyKeyValues[i + 1] == null)) {
                continue;
            }
            final String key = (String) propertyKeyValues[i];
            final Object value = propertyKeyValues[i + 1];

            // Key cannot be empty, must be non-empty String.
            if (key.isEmpty()) {
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            }
            // If cardinality is single we must only retain the final item.
            if (this.features().vertex().getCardinality(key).equals(VertexProperty.Cardinality.single)) {
                properties = properties.stream().filter(p -> !key.equals(p.getKey())).collect(Collectors.toList());
            }
            properties.add(new AbstractMap.SimpleEntry<>(key, value));
        }

        return properties;
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        if (!LocalGraphComputer.class.isAssignableFrom(graphComputerClass))
            throw new IllegalArgumentException(graphComputerClass.getSimpleName() + " is not assignable from " + LocalGraphComputer.class.getSimpleName());
        else {
            try {
                Class<C> clazz = graphComputerClass.equals(GraphComputer.class) ? (Class<C>) LocalGraphComputer.class : graphComputerClass;
                return clazz.getConstructor(FireflyGraph.class).newInstance(this);
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return new LocalGraphComputer(this);
    }

    /**
     * This function finds TinkerPop Element objects in an object array and converts them to ids
     *
     * @param elements array of objects that might be Elements
     * @return array of raw ids
     */
    private List<Object> getIds(final List<Object> elements) {
        return elements.stream().map(e -> {
            if (Element.class.isAssignableFrom(e.getClass())) {
                return ((Element) e).id();
            } else {
                return e;
            }
        }).collect(Collectors.toList());
    }

    public Iterator<Vertex> vertices(final List<String> requiredProperties, final Object... vertexIdsOrVertices) {
        return vertices(List.of(), requiredProperties, vertexIdsOrVertices);
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIdsOrVertices) {
        final Iterator<Vertex> iterator = vertices(List.of(), null, vertexIdsOrVertices);
        // TODO: GRAPH COMPUTER INTERCEPTION
        return FireflyHelper.inComputerMode(this) ?
                FireflyCloseableIteratorUtils.filter(iterator, vertex -> this.graphComputerView.legalVertex(vertex)) :
                iterator;
    }

    public Iterator<Vertex> vertices(final List<HasContainer> filters, final List<String> requiredProperties, final Object... vertexIdsOrVertices) {
        if (vertexIdsOrVertices.length == 1 && vertexIdsOrVertices[0] instanceof String) {
            if (vertexIdsOrVertices[0].equals(FIREFLY_CONFIGURATION_VARIABLE_NAME)) {
                return FireflyCloseableIteratorUtils.of(new FireflyMetadataVertex(this));
            }
            if (vertexIdsOrVertices[0].equals(GRAPH_SUMMARY_VERTEX)) {
                return FireflyCloseableIteratorUtils.of(new FireflyGraphSummaryVertex(this));
            }
            if (vertexIdsOrVertices[0].equals(FIREFLY_WARMUP_VARIABLE_NAME)) {
                WarmupUtil.create(configuration).preheat(48);
                return FireflyCloseableIteratorUtils.of(new FireflyMetadataVertex(this));
            }
        }

        final List<FireflyId> idList = getIds(Arrays.asList(vertexIdsOrVertices)).stream()
                .map(id -> getIdFactory().createId(id, FireflyVertex.class))
                .collect(Collectors.toList());

        // Create vertex iterator with graph and vertex id iterator.
        // If there are vertexIds present use them, otherwise read from database.
        return new FireflyBatchElementIterator<>(this, idList.isEmpty() ? GraphQuery.create(this).scanVertexIds() : idList.iterator(), filters, this::readVertices, requiredProperties);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        final Iterator<Edge> iterator = edges(List.of(), edgeIds);
        // TODO: GRAPH COMPUTER INTERCEPTION
        return FireflyHelper.inComputerMode(this) ?
                FireflyCloseableIteratorUtils.filter(iterator, edge -> this.graphComputerView.legalEdge(edge.outVertex(), edge)) :
                iterator;
    }

    private Iterator<Edge> edges(final List<HasContainer> filters, final Object... edgeIds) {
        // Create edge iterator with graph and edge id iterator.
        // If there are edgeIds present, convert them to an iterator of Longs, otherwise read edges from database.
        final List<Object> ids = getIds(List.of(edgeIds));
        final List<FireflyId> idList;
        try {
            idList = ids.stream()
                    .map(id -> getIdFactory().createId(id, FireflyEdge.class))
                    .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            throw new NoSuchElementException(e.getMessage());
        }

        if (idList.isEmpty()) {
            return new FireflyBatchElementIterator<>(this, GraphQuery.create(this).scanEdgeIds(), filters, this::readEdges, null);
        } else {
            return FireflyEdge.readEdges(this, idList).stream().map(fireflyEdge -> (Edge) fireflyEdge).iterator();
        }
    }

    public interface TransformKeyRecord<E> {
        E transform(final KeyRecord keyRecord);
    }

    public interface GetElements<E extends Element> {
        Iterator<E> getFiltered(final List<HasContainer> hasContainers, final Object... ids);

        Iterator<E> getUnfiltered(final Object... ids);
    }

    /**
     * Function to create vertex property indexes using a list of property keys.
     *
     * @param vertexPropertyIndexes List of vertex property indexes to create.
     */
    private void createIndexes(final Class<? extends FireflyElement> elementClass, final String binName, final String prefix, final List<String> vertexPropertyIndexes) {
        final List<String> existingIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db.getClient(), db.getNamespace()).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());

        // Add indexes specified in properties file.
        for (final String index : vertexPropertyIndexes) {
            // Create both string and numeric indexes for vertex properties.
            final String formattedIndex = String.format("%s_%s", prefix, index);
            db.createIndexBackground(existingIndexes, db.setFromElementType(elementClass),
                    formattedIndex + "_" + STRING, binName, STRING, IndexCollectionType.DEFAULT, false,
                    CTX.mapKey(Value.get(index)));
            db.createIndexBackground(existingIndexes, db.setFromElementType(elementClass),
                    formattedIndex + "_" + NUMERIC, binName, NUMERIC, IndexCollectionType.DEFAULT, false,
                    CTX.mapKey(Value.get(index)));
        }

        // Manually force metadata to update.
        fireflyIndexMetadata.updateMetadata();
    }

    public boolean isEmpty() {
        final FireflyGraphSummaryUpdater.FireflyElementMetadata metadata = this.fireflySummaryUpdater.getFireflyStatistics();
        return metadata.totalEdgeCount() == 0 && metadata.totalVertexCount() == 0;
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException(UNIMPLEMENTED);
    }

    @Override
    public void close() {
        LOG.info("Closing FireflyGraph.");
        this.closed.set(true);
        this.fireflyCardinalityMetadataTask.cancel();
        this.fireflyIndexMetadataTask.cancel();
        this.fireflySummaryUpdater.close();
        this.ttlHandler.close();
        this.db.close();
    }

    @Override
    public Variables variables() {
        return variables;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, db.toString());
    }

    @Override
    public ServiceRegistry getServiceRegistry() {
        return this.serviceRegistry;
    }
}
