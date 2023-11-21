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
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.AerospikeLogger;
import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.ReadContext;
import com.aerospike.firefly.process.call.usage.FireflyUsageStatsServiceFactory;
import com.aerospike.firefly.runtime.tasks.FireflyUsageStats;
import com.aerospike.firefly.structure.util.FireflyTtlHandler;
import com.aerospike.firefly.util.GraphFactory;
import com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException;
import com.aerospike.firefly.runtime.exceptions.VertexRecordSizeExceededException;
import com.aerospike.firefly.jsr223.FireflyGremlinPlugin;
import com.aerospike.firefly.process.call.bulkload.FireflyBulkLoaderServiceFactory;
import com.aerospike.firefly.process.call.FireflyMetadataServiceFactory;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException;
import com.aerospike.firefly.process.computer.FireflyGraphComputerView;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyContentionHandlingStrategy;
import com.aerospike.firefly.structure.id.BufferedNumericIdManager;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.id.IdManager;
import com.aerospike.firefly.structure.id.RecyclingBufferedNumericIdManager;
import com.aerospike.firefly.structure.iterator.FireflyBatchElementIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.runtime.tasks.FireflyMetadataTask;
import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.runtime.HealthcheckServer;
import com.aerospike.firefly.util.LoggerUtil;
import com.aerospike.firefly.util.PluginUtil;
import com.aerospike.firefly.util.WarmupUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.SupportedValueTypes;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getSupportedType;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.structure.FireflyVertex.SUPERNODE_PROPERTY_KEY;
import static com.aerospike.firefly.util.Tokens.EDGE_RECYCLED_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.EDGE_UNIQUE_ID_COUNTER;
import static com.aerospike.firefly.structure.FireflyGraphSummaryVertex.GRAPH_SUMMARY_VERTEX;
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

// Tests that require lambda support.
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV1d0Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV2d0Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV3d0Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoV1d0Test", method = "shouldSerializePathAsDetached", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoV3d0Test", method = "shouldSerializePathAsDetached", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})

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

    public static String FIREFLY_VERSION = "1.1.0";
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
    protected FireflyGraphComputerView graphComputerView = null;
    public final IdManager<Long> vertexIdManager;
    public final IdManager<byte[]> edgeIdManager;
    public final IdManager<Long> vertexPropertyIdManager;
    private final FireflyTtlHandler ttlHandler;
    public final FireflyCardinalityMetadata fireflyCardinalityMetadata;
    public final FireflyUsageStats fireflyUsageStats;;
    public final FireflyIndexMetadata fireflyIndexMetadata;
    public final FireflyGraphSummaryUpdater fireflySummaryUpdater;
    private final ServiceRegistry serviceRegistry = new ServiceRegistry();
    public static final String DOCKER_SETTINGS_FILE_LOCATION = "/opt/aerospike-firefly/conf/firefly-gremlin-server.yaml";

    // Note, this should be overwritten by the settings file contents, but for testing we need a default.
    private final Settings gremlinServerSettings;

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
        serviceRegistry.registerService(new FireflyMetadataServiceFactory(this));
        serviceRegistry.registerService(new FireflyBulkLoaderServiceFactory());
        serviceRegistry.registerService(new FireflyUsageStatsServiceFactory());
        if (conf.containsKey(ConfigurationHelper.Keys.PLUGIN)) {
            PluginUtil.loadPlugin(conf.getString(ConfigurationHelper.Keys.PLUGIN), conf, this);
        }

        // Create usage statistics background task.
        fireflyUsageStats = new FireflyUsageStats(db);
    }

    public static FireflyGraph open(final Configuration conf) {
        final String logLevel = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.LOG_LEVEL, conf);
        final boolean clientLogging = Boolean.parseBoolean(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.ASCLIENT_LOG_ENABLED, conf));
        final boolean preheat = Boolean.parseBoolean(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AUTO_PRE_HEAT, conf));
        try {
            if (clientLogging) {
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
            if (System.getenv("FIREFLY_TESTING") == null ||
                    !System.getenv("FIREFLY_TESTING").equalsIgnoreCase("true")) {
                final Runtime javaRuntime = Runtime.getRuntime();
                LOG.info("Java Runtime: {} available processors.", javaRuntime.availableProcessors());
                LOG.info("Java Runtime: {} MB max memory.", javaRuntime.maxMemory() / (1024 * 1024));
                LOG.info("Java Runtime: {} MB total memory.", javaRuntime.totalMemory() / (1024 * 1024));
                LOG.info("Java Runtime: {} MB free memory.", javaRuntime.freeMemory() / (1024 * 1024));
                LOG.info("JVM Vendor: {}.", System.getProperty("java.vm.vendor"));
                LOG.info("JVM Specification Vendor: {}.", System.getProperty("java.vm.specification.vendor"));
                LOG.info("Java Specification Version: {}.", System.getProperty("java.specification.version"));
                LOG.info("JVM Runtime: {}.", System.getProperty("java.runtime.name"));
                LOG.info("JVM Runtime Version: {}.", System.getProperty("java.runtime.version"));

                // Straight up printing out conf just provides a class name / memory address.
                final Iterator<String> keys = conf.getKeys();
                final Map<String, Object> configurationMap = new HashMap<>();
                while (keys.hasNext()) {
                    final String key = keys.next();
                    configurationMap.put(key, conf.getProperty(key));
                }
                LOG.info("Aerospike Graph Service configuration: {}.", configurationMap);
            }
            LOG.info("Starting Aerospike Graph Service v{}.", FIREFLY_VERSION.replace("-SNAPSHOT", ""));
            if (preheat)
                WarmupUtil.create(conf).preheat(WarmupUtil.passes);
            // Only start healthcheck server if bulk loader is not present in configuration
            if (!Boolean.parseBoolean(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.BULK_LOADER_FLAG, conf)))
                FireflyGremlinPlugin.startHealthcheckServer(conf, HealthcheckServer.DEFAULT_HEALTHCHECK_PORT);
            return GraphFactory.createGraph(AerospikeConnection.connect(conf), conf);
        } catch (Exception e) {
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

    public static synchronized Settings getGremlinServerSettings() {
        if (GREMLIN_SERVER_SETTINGS == null) {
            // We want to load the docker file if it exists, however in our testing it won't, so we can just default the values.
            if (new File(DOCKER_SETTINGS_FILE_LOCATION).exists()) {
                LOG.info("Loading configuration from docker settings file '" + DOCKER_SETTINGS_FILE_LOCATION + "'.");
                try {
                    GREMLIN_SERVER_SETTINGS = Settings.read(DOCKER_SETTINGS_FILE_LOCATION);
                } catch (Exception e) {
                    LOG.error("Failed to load docker settings file '" + DOCKER_SETTINGS_FILE_LOCATION + "'.", e);
                }
            }
            GREMLIN_SERVER_SETTINGS = new Settings();
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

    protected Iterator<FireflyId> scanAllVertices() {
        FireflyGraph.LOG.trace("Scanning {} ids.", db.VERTEX_AERO_SET);
        final Iterator<KeyRecord> i = db.scanAllKeysInSet(ReadContext.create(db.VERTEX_AERO_SET), null);
        return FireflyCloseableIteratorUtils.map(i, r -> getIdFactory().createId(r.key.userKey.getObject(), FireflyVertex.class));
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
        final String counterBinName = direction == Direction.IN ? this.db.IN_EDGE_COUNTER_BIN : this.db.OUT_EDGE_COUNTER_BIN;

        // Simple bin to increment the edge cache counter.
        final Bin incrementEdgeCountBin = new Bin(counterBinName, edgeIds.size());

        // Create the operations.
        final Operation incrementEdgeCount = Operation.add(incrementEdgeCountBin);
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
            this.db.operate(writePolicy, key, incrementEdgeCount, appendEdgeId);
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
        final List<FireflyVertex> vertices = readVertices(List.of(), List.of(idValue));
        if (vertices.isEmpty()) {
            return null;
        } else {
            return vertices.get(0);
        }
    }

    public List<FireflyVertex> readVertices(final List<HasContainer> hasContainers, final List<FireflyId> idValues) {
        return FireflyVertex.readVertices(this, hasContainers, idValues);
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

        final Map<String, Object> data = new TreeMap<>();
        final Map<String, Object> typeHints = new TreeMap<>();
        properties.forEach(prop -> {
            final String key = prop.getKey();
            final Object value = prop.getValue();
            FireflyHelper.validatePropertyValue(value);

            if (value == null) {
                data.remove(key);
                typeHints.remove(key);
            } else {
                typeHints.put(key, getSupportedType(value));
                data.put(key, value);
            }
        });

        final List<Operation> operations = new ArrayList<>();
        // CREATE and UPDATE are both okay since this is idempotent.
        final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation writeLabel = MapOperation.put(mapPolicy,db.LABEL_BIN,
                Value.get(edgeId), Value.get(label));
        operations.add(writeLabel);

        final Operation writeInV = MapOperation.put(mapPolicy, Direction.IN.name(),
                Value.get(edgeId), Value.get(FireflyIdPoly.fromObject(inVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
        operations.add(writeInV);
        final Operation writeOutV = MapOperation.put(mapPolicy, Direction.OUT.name(),
                Value.get(edgeId), Value.get(FireflyIdPoly.fromObject(outVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
        operations.add(writeOutV);

        // Write to supernodes bin if vertex cache overflowed.
        if (inVSupernode) {
            final Operation writeInVSupernode = MapOperation.put(mapPolicy, db.SUPERNODES_IN_BIN,
                    Value.get(edgeId), Value.get(FireflyIdPoly.fromObject(inVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
            operations.add(writeInVSupernode);
        }
        if (outVSupernode) {
            final Operation writeOutVSupernode = MapOperation.put(mapPolicy, db.SUPERNODES_OUT_BIN,
                    Value.get(edgeId), Value.get(FireflyIdPoly.fromObject(outVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
            operations.add(writeOutVSupernode);
        }

        final Operation writeProperties = MapOperation.put(mapPolicy, db.PROPERTIES_BIN,
                Value.get(edgeId), Value.get(data, MapOrder.KEY_ORDERED));
        operations.add(writeProperties);
        final Operation writeTypeHints = MapOperation.put(mapPolicy, db.TYPE_HINTS_BIN,
                Value.get(edgeId), Value.get(typeHints, MapOrder.KEY_ORDERED));
        operations.add(writeTypeHints);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        writePolicy.maxRetries = db.AEROSPIKE_WRITE_MAX_RETRY;
        final Key key = getKey(db, db.EDGE_AERO_SET, getIdFactory().createId(edgeId, FireflyEdge.class));
        try {
            db.operate(writePolicy, key, operations.toArray(new Operation[0]));
        } catch (final EdgeRecordSizeExceededException ersee) {
            throw new FireflyLoadingException((AerospikeException) ersee.getCause(), ersee.getMessage());
        } catch (final AerospikeException ae) {
            throw new FireflyLoadingException(ae);
        }
        fireflySummaryUpdater.addEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
    }

    /**
     * Function to remove edge record via id without reading the edge back.
     * NOTE: This function does not remove the edge from adjacent vertices. This must be done separately.
     *
     * @param edgeId Id of edge to remove.
     */
    public void removeEdgeById(final FireflyId edgeId) {
        // Remove edge.
        FireflyGraph.LOG.debug("Removing edge {}.", edgeId);

        FireflyEdge.removeEdgeById(this, edgeId);
    }

    /**
     * Function to read edges from Aerospike.
     *
     * @param edgeIds Edge ids.
     * @return Edge.
     */
    public List<FireflyEdge> readEdges(final List<HasContainer> hasContainers, final List<FireflyId> edgeIds) {
        if (!hasContainers.isEmpty()) {
            throw new RuntimeException("Pushdown is not currently supported for Edges.");
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
                    typeHints.put((String) keyValues[i], getSupportedType(keyValues[i + 1]));
                } else if (allowNullProperties) {
                    properties.put((String) keyValues[i], keyValues[i + 1]);
                    typeHints.put((String) keyValues[i], SupportedValueTypes.get(String.class));
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

    public long getVertexCount(final Expression expression) {
        return FireflyCloseableIteratorUtils.count(db.scanAllKeysInSet(ReadContext.create(db.VERTEX_AERO_SET), expression, false));
    }

    public long getEdgeCount() {
        return FireflyCloseableIteratorUtils.count(this.db.readElementIds(FireflyEdge.class));
    }

    @Override
    public AerospikeConnection getBaseGraph() {
        return db;
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
                } catch (AerospikeException e) {
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
            } catch (AerospikeException e) {
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
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new UnsupportedOperationException(UNIMPLEMENTED);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException(UNIMPLEMENTED);
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

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIdsOrVertices) {
        return vertices(List.of(), vertexIdsOrVertices);
    }

    public Iterator<Vertex> vertices(final List<HasContainer> filters, final Object... vertexIdsOrVertices) {
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
        return new FireflyBatchElementIterator<>(this, idList.isEmpty() ? scanAllVertices() : idList.iterator(), filters, this::readVertices);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return edges(List.of(), edgeIds);
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
            return new FireflyBatchElementIterator<>(this, this.db.readElementIds(FireflyEdge.class), filters, this::readEdges);
        } else {
            return FireflyEdge.readEdges(this, idList).stream().map(fireflyEdge -> (Edge) fireflyEdge).iterator();
        }
    }

    public void scheduleElementForTtlNow(final FireflyElement element, final long timeToLiveMillis) {
        this.ttlHandler.scheduleExpiryNow(element, timeToLiveMillis);
    }

    /**
     * Create an Aerospike index Filter using the predicate and index info.
     *
     * @param predicate Predicate to use.
     * @param indexInfo Index info to use.
     * @return
     */
    public Filter predicateToFilter(final P<?> predicate, final FireflyIndexMetadata.IndexInfo indexInfo) {
        final String name;
        if (db.LABEL_BIN.equals(indexInfo.key)) {
            name = db.LABEL_BIN;
        } else if (indexInfo.setName.equals(getBaseGraph().VERTEX_AERO_SET)) {
            name = db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN;
        } else if (indexInfo.setName.equals(getBaseGraph().EDGE_AERO_SET)) {
            name = db.PROPERTIES_BIN;
        } else {
            throw new IllegalArgumentException(
                    "Cannot create filter for index with unknown set name: " + indexInfo.setName + " and key " + indexInfo.key);
        }

        final IndexCollectionType type = db.LABEL_BIN.equals(indexInfo.key) ?
                IndexCollectionType.DEFAULT : IndexCollectionType.MAPVALUES;
        final Object value = predicate.getValue();
        if (Number.class.isAssignableFrom(value.getClass())) {
            final Long casted;
            if (Integer.class.isAssignableFrom(value.getClass())) {
                casted = Long.valueOf((Integer) value);
            } else if (Long.class.isAssignableFrom(value.getClass())) {
                casted = (Long) value;
            } else {
                throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
            }

            if (predicate.getBiPredicate().equals(Compare.eq)) {
                return Filter.equal(name, casted, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.lt)) {
                return Filter.range(name, Long.MIN_VALUE, casted - 1, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.lte)) {
                return Filter.range(name, Long.MIN_VALUE, casted, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.gt)) {
                return Filter.range(name, casted - 1, Long.MAX_VALUE, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.gte)) {
                return Filter.range(name, casted, Long.MAX_VALUE, CTX.mapKey(Value.get(indexInfo.key)));
            } else {
                throw new RuntimeException(String.format("%s not a supported predicate", predicate));
            }
        } else {
            if (db.LABEL_BIN.equals(indexInfo.key)) {
                return Filter.contains(name, type, (String) value);
            } else {
                return Filter.equal(name, (String) value, CTX.mapKey(Value.get(indexInfo.key)));
            }
        }
    }

    /**
     * Create an Aerospike Expression from the predicate, map key, and bin name.
     *
     * @param binName   Bin name to use.
     * @param mapKey    Map key to use.
     * @param predicate Predicate to use.
     * @return Expression.
     */
    private Exp predicateToExpression(final String binName,
                                      final String mapKey,
                                      final P<?> predicate) {
        // If the bin is the label bin, we can make a very simple predicate.
        if (db.LABEL_BIN.equals(binName)) {
            return Exp.eq(Exp.stringBin(db.LABEL_BIN), Exp.val((String) predicate.getValue()));
        }

        // Need to build a more complex expression for nested map values.
        final Object value = predicate.getValue();
        if (Number.class.isAssignableFrom(value.getClass())) {
            final Long casted;
            if (Integer.class.isAssignableFrom(value.getClass())) {
                casted = Long.valueOf((Integer) value);
            } else if (Long.class.isAssignableFrom(value.getClass())) {
                casted = (Long) value;
            } else {
                throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
            }

            if (predicate.getBiPredicate().equals(Compare.eq)) {
                return Exp.eq(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.lt)) {
                return Exp.lt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.lte)) {
                return Exp.le(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.gt)) {
                return Exp.gt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.gte)) {
                return Exp.ge(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else {
                throw new RuntimeException(String.format("%s not a supported predicate", predicate));
            }
        } else {
            return Exp.eq(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val((String) value));
        }
    }

    public Expression hasContainerListToExpression(final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        if (hasContainers.size() == 0) {
            return null;
        }
        final Exp[] exps = hasContainers.stream().map(h ->
                predicateToExpression(h.getKey().equals("~label") ?
                                db.LABEL_BIN : FireflyVertex.class.isAssignableFrom(clazz) ?
                                db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN : db.PROPERTIES_BIN,
                        h.getKey(),
                        h.getPredicate())).toArray(Exp[]::new);
        return exps.length == 1 ? Exp.build(exps[0]) : Exp.build(Exp.and(exps));
    }

    private Exp[] hasContainerListToExpArray(final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        return hasContainers.stream().map(h ->
                predicateToExpression(h.getKey().equals("~label") ?
                                db.LABEL_BIN : FireflyVertex.class.isAssignableFrom(clazz) ?
                                db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN : db.PROPERTIES_BIN,
                        h.getKey(),
                        h.getPredicate())).toArray(Exp[]::new);
    }

    /**
     * Execute query on index with predicate and return on the fly transformed iterator.
     *
     * @param indexInfo Index info to use.
     * @param predicate Predicate to use.
     * @param transform Transform to use.
     * @param <E>       Type of element to return.
     * @return Iterator of transformed elements.
     */
    public <E extends Element> Iterator<E> queryIndex(final FireflyIndexMetadata.IndexInfo indexInfo,
                                                      final P<?> predicate,
                                                      final TransformKeyRecord<E> transform,
                                                      final List<HasContainer> hasContainers,
                                                      final Class<? extends FireflyElement> clazz) {
        // Create query policy with expressions.
        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.filterExp = hasContainerListToExpression(hasContainers, clazz);

        // Query index.
        final Iterator<KeyRecord> keyRecordIterator = db.queryIndex(indexInfo.setName, indexInfo.indexName, predicateToFilter(predicate, indexInfo), queryPolicy);

        // Transform record to correct element.
        return FireflyCloseableIteratorUtils.map(keyRecordIterator, transform::transform);
    }

    /**
     * Execute query on index with predicate and return on the fly transformed iterator.
     *
     * @param indexInfo Index info to use.
     * @param predicate Predicate to use.
     * @param transform Transform to use.
     * @param <E>       Type of element to return.
     * @return Iterator of transformed elements.
     */
    public <E extends Element> Iterator<E> queryIndex(final FireflyIndexMetadata.IndexInfo indexInfo,
                                                      final P<?> predicate,
                                                      final TransformKeyRecord<E> transform) {
        return queryIndex(indexInfo, predicate, transform, Collections.emptyList(), null);
    }

    /**
     * Execute query on scan with predicate, map key, and return on the fly transformed iterator.
     *
     * @param mapKey        Map key to use.
     * @param setName       Set name to use.
     * @param binName       Bin name to use.
     * @param predicate     Predicate to use.
     * @param transform     Transform to use.
     * @param hasContainers HasContainers to use
     * @param <E>           Type of element to return.
     * @return Iterator of transformed elements.
     */
    public <E extends Element> Iterator<E> queryScan(final String mapKey,
                                                     final String setName,
                                                     final String binName,
                                                     final P<?> predicate,
                                                     final TransformKeyRecord<E> transform,
                                                     final List<HasContainer> hasContainers,
                                                     final Class<? extends FireflyElement> clazz) {
        // Build expression using predicate.
        final Exp exp = predicateToExpression(binName, mapKey, predicate);
        final Expression expression;
        if (hasContainers.size() > 0) {
            final Exp[] exps = hasContainerListToExpArray(hasContainers, clazz);
            final Exp[] allExps = new Exp[exps.length + 1];
            allExps[0] = exp;
            System.arraycopy(exps, 0, allExps, 1, exps.length);
            expression = Exp.build(Exp.and(allExps));
        } else {
            expression = Exp.build(exp);
        }

        db.getScanHitCounter().increment(mapKey);

        final ScanPolicy policy = new ScanPolicy();
        final Iterator<KeyRecord> keyRecordIterator = db.scanAllRecordsInSet(ReadContext.create(setName, binName, mapKey), expression, policy);

        // Transform record to correct element.
        return FireflyCloseableIteratorUtils.map(keyRecordIterator, kr -> transform.transform(kr));
    }

    /**
     * Execute query on scan with predicate, map key, and return on the fly transformed iterator.
     *
     * @param mapKey    Map key to use.
     * @param setName   Set name to use.
     * @param binName   Bin name to use.
     * @param predicate Predicate to use.
     * @param transform Transform to use.
     * @param <E>       Type of element to return.
     * @return Iterator of transformed elements.
     */
    public <E extends Element> Iterator<E> queryScan(final String mapKey,
                                                     final String setName,
                                                     final String binName,
                                                     final P<?> predicate,
                                                     final TransformKeyRecord<E> transform) {
        return queryScan(mapKey, setName, binName, predicate, transform, List.of(), FireflyVertex.class);
    }

    /**
     * Template to fill out to allow on the fly KeyRecord to Element mapping.
     *
     * @param <E> Type of element to return.
     */
    public interface TransformKeyRecord<E extends Element> {
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
            db.createKeyValueSindex(existingIndexes, db.setFromElementType(elementClass),
                    formattedIndex + "_" + STRING, binName, index, STRING, IndexCollectionType.DEFAULT);
            db.createKeyValueSindex(existingIndexes, db.setFromElementType(elementClass),
                    formattedIndex + "_" + NUMERIC, binName, index, NUMERIC, IndexCollectionType.DEFAULT);
        }

        // Manually force metadata to update.
        fireflyIndexMetadata.updateMetadata();
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
