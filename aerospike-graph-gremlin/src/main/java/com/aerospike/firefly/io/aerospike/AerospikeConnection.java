package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.query.ReadInfo;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.DiagnosticUtil;
import com.aerospike.firefly.util.WarmupUtil;
import com.aerospike.firefly.util.config.FireflyConfiguration;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.structure.FireflyGraph.EP_INDEX_PREFIX;
import static com.aerospike.firefly.structure.FireflyGraph.VP_INDEX_PREFIX;
import static com.aerospike.firefly.structure.util.FireflyTtlHandler.TTL_TIME_KEY;
import static com.aerospike.firefly.util.config.ConfigurationHelper.IMMUTABLE_CONFIG_KEYS;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.BULK_LOADER_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.BULK_LOADER_INITIALIZER_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.MRT_ENABLED_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.getOrDefaultString;
import static com.aerospike.firefly.util.exceptions.AerospikeGraphException.fromAerospikeException;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class AerospikeConnection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeConnection.class);
    private final IAerospikeClient client;
    private final EventLoops eventLoops;
    private final ExecutorService threadedReadExecutor;

    static {
        Value.UseBoolBin = true;
    }

    public static final String DATA_MODEL_KEY = "DATA_MODEL_KEY";
    public static final String DATA_MODEL_NAME = "DATA_MODEL_NAME";
    public static final String DATA_MODEL_VER = "DATA_MODEL_VER";
    public static final String DATA_MODEL_CONF = "DATA_MODEL_CONF";

    public final String GRAPH_ID;
    public final boolean MRT_ENABLED;
    public final int MRT_TIMEOUT;
    public final String V_LABEL_INDEX_NAME;
    public final String E_LABEL_INDEX_NAME;
    public final boolean V_LABEL_INDEX_ENABLED_FLAG;
    public final boolean E_LABEL_INDEX_ENABLED_FLAG;
    public final String E_IN_INDEX_NAME;
    public final String E_OUT_INDEX_NAME;
    public final String namespace;

    public final String USER_KEY_BIN;

    public final String LABEL_BIN;
    public final String IN_EDGES_BIN;
    public final String OUT_EDGES_BIN;
    public final String EDGE_CACHE_DISABLED_BIN;
    public final String RELATIONAL_VERTEX_TYPE_HINT_BIN;
    public final String INDEX_METADATA_SET;

    public final String GRAPH_METADATA_SET;
    public final String BULK_LOAD_METADATA_SET;
    public final String BULK_LOAD_DUPLICATE_VID_SET;
    public final String BULK_LOAD_BAD_EDGE_SET;
    public final String BULK_LOAD_BAD_ENTRY_SET;
    public final String BULK_LOAD_RECOVERY_VERTEX_SET;
    public final String BULK_LOAD_RECOVERY_EDGE_SET;
    public final String BULK_LOAD_RECOVERY_SUPERNODE_SET;
    public final String BULK_LOAD_RECOVERY_STATE_SET;
    public final String BULK_LOAD_RECOVERY_BIN;
    public final String GRAPH_VARIABLES_SET;
    public final Object GRAPH_VARIABLES_REC_KEY;
    public final String GRAPH_VARIABLES_BIN;
    public final String EDGE_AERO_SET;

    public final String VERTEX_AERO_SET;
    public final String IN_VP_SET;
    public final String OUT_VP_SET;
    public final String SUMMARY_SET;
    public final String VERTEX_PROPERTY_NAME_TO_ID_BIN;
    public final String VERTEX_PROPERTY_NAME_TO_VALUE_BIN;
    public final String VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN;
    public final String USAGE_STATS_SET;
    public final String USAGE_STATS_BIN;
    public final long ON_RECORD_ID_LIMIT;
    public final String PROPERTIES_BIN;
    public final String TYPE_HINTS_BIN;
    public final String COUNTER_BIN;
    public final String ID_MANAGER_SET;
    public final String ID_TYPE_BIN;
    public final String TEST_SET;
    public final String OLAP_SET;
    public final String OLAP_LIMIT_BIN;

    public final Configuration conf;
    public final String USER_SUPPLIED_ID_CACHE_SET;
    public final long CARDINALITY_METADATA_UPDATE_FREQUENCY;
    public final long INDEX_METADATA_UPDATE_FREQUENCY;
    public final String SUPERNODES_IN_BIN;
    public final String SUPERNODES_OUT_BIN;
    public final String SUPERNODE_EDGE_PROPERTIES_BIN;
    public final String BL_ROW_BIN;
    public final String BL_FILE_BIN;
    public final boolean GLOBAL_EDGE_CACHE_ENABLED_FLAG;
    public final ThreadLocal<FireflyCache> transactionCache = new ThreadLocal<>();
    public final ThreadLocal<FireflyCache> emptyPropsTransactionCache = new ThreadLocal<>();
    public final ThreadLocal<ScanHitCounter> scanHitCounterThreadLocal = new ThreadLocal<>();
    public final int AEROSPIKE_BATCH_READ_SIZE;
    public final int AEROSPIKE_BATCH_THRESHOLD;
    public final long FIREFLY_READ_THROUGH_CACHE_WEIGHT;
    public final int PHAT_EDGE_SIZE;
    public final int MOVEMENT_BARRIER_SIZE;
    public final boolean SUMMARY_TICKER_ENABLED_FLAG;
    public final boolean SUMMARY_ENABLED_FLAG;
    public final boolean TTL_ENABLED_FLAG;
    public final String TTL_BIN;
    public final String EDGE_DATA_BIN;
    public final String TTL_VERTEX_INDEX_NAME;
    public final String TTL_EDGE_INDEX_NAME;
    public final int TTL_PURGE_INTERVAL_SECONDS;
    public final boolean SUPERNODE_TRAVERSAL_LOG_WARNING;
    public final boolean REDACT_SCRIPT_LITERALS_ENABLED;
    public long lastQueryMissCount = 0; // For testing
    public long lastQueryHitCount = 0; // For testing

    private final int AEROSPIKE_MAX_RETRIES;
    private final int WRITE_SLEEP_BETWEEN_RETRY;
    private final int READ_SLEEP_BETWEEN_RETRY;
    private final int WRITE_TOTAL_TIMEOUT;
    private final int READ_TOTAL_TIMEOUT;
    private final int READ_TOTAL_TIMEOUT_BULK_LOAD;
    private final int WRITE_SOCKET_TIMEOUT;
    private final int READ_SOCKET_TIMEOUT;
    private final int READ_SOCKET_TIMEOUT_BULK_LOAD;
    private final int CONNECT_TIMEOUT;
    private final int TIMEOUT_DELAY;

    private final int SCAN_TOTAL_TIMEOUT;
    private final int SCAN_SOCKET_TIMEOUT;
    private final int SCAN_CONNECT_TIMEOUT;
    private final int SCAN_TIMEOUT_DELAY;

    private final int INDEX_TOTAL_TIMEOUT;
    private final int INDEX_SOCKET_TIMEOUT;
    private final int INDEX_CONNECT_TIMEOUT;
    private final int INDEX_TIMEOUT_DELAY;

    public final long PROPERTY_ID_BUFFER_SIZE;
    public final long VERTEX_ID_BUFFER_SIZE;
    public final long EDGE_ID_BUFFER_SIZE;
    public final long USAGE_STATS_UPDATE_INTERVAL;
    public final boolean WARMUP_MODE;
    public final boolean PROMETHEUS_RENAME_ENABLED;

    // Bulk Loader fields
    public final Object BL_DUPLICATE_VERTEX_COUNT_KEY;
    public final Object BL_BAD_EDGES_COUNT_KEY;
    public final Object BL_BAD_ENTRY_COUNT_KEY;

    public static final AtomicLong instanceCounter = new AtomicLong(0);

    private final FireflyIdFactory idFactory;
    public final boolean ENABLE_COMPOSITE_ID_STRATEGY;
    public final boolean ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY;
    public final boolean ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY;
    public final boolean ENABLE_COMPOSITE_ID_LIMIT_STRATEGY;
    public final boolean ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY;
    public final boolean ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY;
    public final boolean ENABLE_BATCH_EDGE_TO_VERTEX_READ_STRATEGY;
    public final boolean ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY;
    public final boolean ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY;
    public final boolean ENABLE_CACHED_ADJACENT_ID_STRATEGY;

    // MergeEdge fields
    public final int MERGE_EDGE_EVAL_TIMEOUT;
    public final int MERGE_EDGE_TTL;
    public final int MERGE_EDGE_POLL_INTERVAL;
    public final boolean MERGE_EDGE_STARVATION_PROTECTION;

    // TODO: Once we are 100% sure these are stable, we can remove the enable flags.
    public final boolean ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY;
    public final boolean ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY;
    public final boolean ENABLE_BATCHED_REPEAT_STEP_STRATEGY;
    public final int PAGINATION_PAGE_QUEUE_SIZE;
    public final int PAGINATION_PAGE_SIZE;
    public final int PAGINATION_PAGE_MAX_WAIT;
    public final int PAGINATION_SHUTDOWN_WAIT;
    public final int OLAP_PAGINATION_WORKERS;
    public final int OLAP_WORKERS;
    public final boolean IS_AUDIT_LOG_ENABLED;
    public final boolean AUTHENTICATION_ENABLED;
    public final boolean USAGE_STATS_SET_INDEX_ENABLED;
    public boolean isSupernodePushdownEnabled = true;
    public boolean isMergeEdgeDataModelEnabled = true;
    public final List<String> vertexNonPropertyBins = new ArrayList<>();
    public final List<String> vertexPropertyBins = new ArrayList<>();

    public final String QUERY_IMPL;

    private final boolean bulkLoaderFlag;
    private final boolean bulkLoaderInitializerFlag;
    private final boolean olapEnabledFlag;

    public static ClientPolicy setupClientPolicy(final Configuration conf, final int threadPoolSize, final EventLoops eventLoops) {
        final ClientPolicy clientPolicy = new ClientPolicy();

        clientPolicy.maxConnsPerNode = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MAX_CONNECTIONS_PER_NODE, conf);
        clientPolicy.minConnsPerNode = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MIN_CONNECTIONS_PER_NODE, conf);
        if (clientPolicy.maxConnsPerNode < clientPolicy.minConnsPerNode) {
            throw new IllegalStateException("Error: 'aerospike.client.clientPolicy.minConnsPerNode' is set to '" +
                    clientPolicy.minConnsPerNode
                    + "' which is greater than 'aerospike.client.clientPolicy.maxConnsPerNode' set to '" +
                    clientPolicy.maxConnsPerNode + "'. 'aerospike.client.clientPolicy.minConnsPerNode' must be less " +
                    "than or equal to 'aerospike.client.clientPolicy.maxConnsPerNode'.");
        }
        clientPolicy.timeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_TIMEOUT, conf);
        clientPolicy.eventLoops = eventLoops;

        // If username and password are not null or empty strings, then set the user and password on the client policy.
        final String user = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_USER, conf);
        final String password = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_PASSWORD, conf);
        if (user != null && !user.equals("") && password != null && !password.equals("")) {
            LOG.info("Setting Aerospike user and password.");
            clientPolicy.user = user;
            clientPolicy.password = password;
        }
        final boolean tlsEnabled = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.TLS, conf);
        if (tlsEnabled) {
            clientPolicy.tlsPolicy = new TlsPolicy();

        }
        clientPolicy.maxErrorRate = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MAX_ERROR_RATE, conf);
        clientPolicy.authMode = AuthMode.valueOf(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AUTH_MODE, conf).toUpperCase());
        clientPolicy.useServicesAlternate = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.CLIENT_SERVICES_ALTERNATE, conf);
        final String clusterName = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.CLUSTER_NAME, conf);
        if (clusterName != null && !clusterName.isBlank()) {
            clientPolicy.clusterName = clusterName;
        }
        // This setting should only be disabled for internal testing use.
        clientPolicy.validateClusterName = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.VALIDATE_CLUSTER_NAME, conf);
        return clientPolicy;
    }

    public static AerospikeClient setupDefaultClient(final Configuration conf, final ClientPolicy policy) {
        final String hostFromConf = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_HOST, conf);
        final String hostsString = stripAllWhiteSpace(hostFromConf);
        final int defaultPort = Integer.parseInt(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_PORT, conf));

        final Host[] hosts = Host.parseHosts(hostsString, defaultPort);

        final AerospikeClient aerospikeClient;
        try {
            aerospikeClient = new AerospikeClient(policy, hosts);
        } catch (final AerospikeException e) {
            LOG.error("Error connecting to Aerospike", e);
            throw fromAerospikeException(e);
        } catch (final Exception e) {
            LOG.error("Error connecting to Aerospike", e);
            throw e;
        }

        FireflyAerospikeVersionCheck.validateVersion(aerospikeClient, false);
        FireflyAerospikeGraphServiceCheck.checkFeatureKey(aerospikeClient);

        if (ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.CLIENT_FAILURE_TEST, conf)) {
            return DiagnosticUtil.enableWriteFails(aerospikeClient, conf);
        } else {
            return aerospikeClient;
        }
    }

    public static String stripAllWhiteSpace(final String hosts) {
        return hosts.replaceAll("\\s+", "");
    }

    public static int getDefaultThreadPoolSize(final Settings gremlinServerSettings) {
        // Min connections per node should be at least the thread pool size.
        // In batching we may use up to 1 connection per node per thread at a time.
        // We must add 2 because both the metadata updater thread and the cardinality metadata threads using the connection.
        //
        // The bulk loader uses 2 * availableProcessors + 4 for buffer + 10 for:
        // - Metadata updater thread
        // - Graph summary reader (via Progress bar)
        // - Cardinality metadata
        // - Index metadata
        // - Graph summary writer
        // - Usage stats writing
        // - Usage stats reading (via prometheus)
        // - TTL thread background worker
        //
        // Because of this, we need to use the greatest of either what the bulk loader would use or what gremlin-server would use.
        return Math.max(2 * Runtime.getRuntime().availableProcessors() + 14, 2 * gremlinServerSettings.gremlinPool + 14);
    }

    /**
     * Construct a new AerospikeConnection
     *
     * @param conf Apache Configuration
     */
    private AerospikeConnection(final FireflyConfiguration conf,
                               final AerospikeClient client,
                               final EventLoops eventLoops,
                               final ExecutorService threadedReadExecutor) {
        LOG.info("Initializing AerospikeConnection.");
        LOG.debug("CONFIGURATION:");
        conf.getKeys().forEachRemaining(key -> {
            if (key.contains("password") || key.contains("secret") || key.contains("token") || key.contains("passkey")) {
                LOG.debug("\tconfig: [{}]:[{}]", key, "********");
            } else {
                LOG.debug("\tconfig: [{}]:[{}]", key, conf.get(String.class, key));
            }
        });
        LOG.debug("Instance counter: {}", instanceCounter.incrementAndGet());

        this.conf = conf;
        this.namespace = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, conf);
        this.eventLoops = eventLoops;
        this.client = client;
        this.threadedReadExecutor = threadedReadExecutor;

        // Verify that the namespace is not using a default-ttl.
        if (InfoOps.getIsAerospikeTTLEnabled(client, namespace)) {
            throw new AerospikeGraphException(GraphError.DEFAULT_TTL_EXISTS);
        }

        V_LABEL_INDEX_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG, conf);
        E_LABEL_INDEX_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG, conf);
        GLOBAL_EDGE_CACHE_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED, conf);
        SUMMARY_TICKER_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SUMMARY_TICKER_ENABLED_FLAG, conf);
        SUMMARY_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SUMMARY_ENABLED_FLAG, conf);
        ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY, conf);
        ENABLE_COMPOSITE_ID_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY, conf);
        ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY, conf);
        ENABLE_COMPOSITE_ID_LIMIT_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_LIMIT_STRATEGY, conf);
        ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY, conf);
        ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY, conf);
        ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY, conf);
        ENABLE_BATCH_EDGE_TO_VERTEX_READ_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_TO_VERTEX_READ_STRATEGY, conf);
        ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY, conf);
        ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY, conf);
        ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY, conf);
        ENABLE_BATCHED_REPEAT_STEP_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCHED_REPEAT_STEP_STRATEGY, conf);
        ENABLE_CACHED_ADJACENT_ID_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY, conf);

        if (ENABLE_CACHED_ADJACENT_ID_STRATEGY && !ENABLE_COMPOSITE_ID_STRATEGY) {
            throw new AerospikeGraphException(GraphError.CACHE_ADJACENT_ENABLED_COMPOSITE_ID_DISABLED);
        }

        TTL_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.TTL_ENABLED_FLAG, conf);
        PAGINATION_PAGE_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, conf);
        PAGINATION_PAGE_MAX_WAIT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_MAX_WAIT, conf);
        PAGINATION_SHUTDOWN_WAIT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_SHUTDOWN_WAIT, conf);
        OLAP_PAGINATION_WORKERS = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.OLAP_PAGINATION_WORKERS, conf);
        OLAP_WORKERS = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.OLAP_WORKERS, conf);
        PAGINATION_PAGE_QUEUE_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_QUEUE_SIZE, conf);
        AUTHENTICATION_ENABLED = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUTHENTICATION_ENABLED, conf);
        USAGE_STATS_SET_INDEX_ENABLED = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.USAGE_STATS_SET_INDEX_ENABLED, conf);
        IS_AUDIT_LOG_ENABLED = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUDIT_LOG_ENABLED, conf);

        if (IS_AUDIT_LOG_ENABLED && !AUTHENTICATION_ENABLED) {
            throw new IllegalStateException("Audit logging requires JWT authentication to be configured.");
        }

        GRAPH_VARIABLES_REC_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.GRAPH_VARIABLES_REC_KEY.name(), conf);
        BL_DUPLICATE_VERTEX_COUNT_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.BL_DUPLICATE_VERTEX_COUNT_KEY.name(), conf);
        BL_BAD_EDGES_COUNT_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.BL_BAD_EDGES_COUNT_KEY.name(), conf);
        BL_BAD_ENTRY_COUNT_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.BL_BAD_ENTRY_COUNT_KEY.name(), conf);

        AEROSPIKE_MAX_RETRIES = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_MAX_RETRIES, conf);
        WRITE_SLEEP_BETWEEN_RETRY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.WRITE_SLEEP_BETWEEN_RETRY, conf);
        READ_SLEEP_BETWEEN_RETRY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_SLEEP_BETWEEN_RETRY, conf);
        WRITE_TOTAL_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.WRITE_TOTAL_TIMEOUT, conf);
        READ_TOTAL_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_TOTAL_TIMEOUT, conf);
        READ_TOTAL_TIMEOUT_BULK_LOAD = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_TOTAL_TIMEOUT_BULK_LOAD, conf);
        WRITE_SOCKET_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT, conf);
        READ_SOCKET_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_SOCKET_TIMEOUT, conf);
        READ_SOCKET_TIMEOUT_BULK_LOAD = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_SOCKET_TIMEOUT_BULK_LOAD, conf);
        CONNECT_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.CONNECT_TIMEOUT, conf);
        TIMEOUT_DELAY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.TIMEOUT_DELAY, conf);

        SCAN_TOTAL_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SCAN_TOTAL_TIMEOUT, conf);
        SCAN_SOCKET_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SCAN_SOCKET_TIMEOUT, conf);
        SCAN_CONNECT_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SCAN_CONNECT_TIMEOUT, conf);
        SCAN_TIMEOUT_DELAY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SCAN_TIMEOUT_DELAY, conf);

        INDEX_TOTAL_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.INDEX_TOTAL_TIMEOUT, conf);
        INDEX_SOCKET_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.INDEX_SOCKET_TIMEOUT, conf);
        INDEX_CONNECT_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.INDEX_CONNECT_TIMEOUT, conf);
        INDEX_TIMEOUT_DELAY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.INDEX_TIMEOUT_DELAY, conf);

        CARDINALITY_METADATA_UPDATE_FREQUENCY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.CARDINALITY_METADATA_UPDATE_FREQUENCY, conf);
        INDEX_METADATA_UPDATE_FREQUENCY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.INDEX_METADATA_UPDATE_FREQUENCY, conf);
        TTL_PURGE_INTERVAL_SECONDS = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.TTL_PURGE_INTERVAL_SECONDS, conf);
        SUPERNODE_TRAVERSAL_LOG_WARNING = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SUPERNODE_TRAVERSAL_LOG_WARNING, conf);
        REDACT_SCRIPT_LITERALS_ENABLED = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.REDACT_SCRIPT_LITERALS_ENABLED, conf);

        TEST_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.TEST_SET.name(), conf);
        OLAP_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.OLAP_SET.name(), conf);
        OLAP_LIMIT_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.OLAP_LIMIT_BIN.name(), conf);
        SUMMARY_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.SUMMARY_SET.name(), conf);
        GRAPH_ID = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.GRAPH_ID, conf);
        VERTEX_AERO_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.VERTEX_AERO_SET.name(), conf);
        IN_VP_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.IN_VP_SET.name(), conf);
        OUT_VP_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.OUT_VP_SET.name(), conf);
        ID_MANAGER_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.ID_MANAGER_SET.name(), conf);
        EDGE_AERO_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.EDGE_AERO_SET.name(), conf);
        GRAPH_METADATA_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.GRAPH_METADATA_SET.name(), conf);
        GRAPH_VARIABLES_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.GRAPH_VARIABLES_SET.name(), conf);
        USAGE_STATS_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.USAGE_STATS_SET.name(), conf);
        INDEX_METADATA_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.INDEX_METADATA_SET.name(), conf);
        USER_SUPPLIED_ID_CACHE_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.USER_SUPPLIED_ID_CACHE_SET.name(), conf);
        BULK_LOAD_METADATA_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_METADATA_SET.name(), conf);
        BULK_LOAD_DUPLICATE_VID_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_DUPLICATE_VID_SET.name(), conf);
        BULK_LOAD_BAD_EDGE_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_BAD_EDGE_SET.name(), conf);
        BULK_LOAD_BAD_ENTRY_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_BAD_ENTRY_SET.name(), conf);
        BULK_LOAD_RECOVERY_VERTEX_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_RECOVERY_VERTEX_SET.name(), conf);
        BULK_LOAD_RECOVERY_EDGE_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_RECOVERY_EDGE_SET.name(), conf);
        BULK_LOAD_RECOVERY_SUPERNODE_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_RECOVERY_SUPERNODE_SET.name(), conf);
        BULK_LOAD_RECOVERY_STATE_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_RECOVERY_STATE_SET.name(), conf);

        E_IN_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.E_IN_INDEX_NAME.name(), conf));
        E_OUT_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.E_OUT_INDEX_NAME.name(), conf));
        V_LABEL_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.V_LABEL_INDEX_NAME.name(), conf));
        E_LABEL_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.E_LABEL_INDEX_NAME.name(), conf));
        TTL_VERTEX_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.TTL_VERTEX_INDEX_NAME.name(), conf));
        TTL_EDGE_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.TTL_EDGE_INDEX_NAME.name(), conf));
        VERTEX_PROPERTY_NAME_TO_ID_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.VERTEX_PROPERTY_NAME_TO_ID_BIN.name(), conf);
        VERTEX_PROPERTY_NAME_TO_VALUE_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.VERTEX_PROPERTY_NAME_TO_VALUE_BIN.name(), conf);
        VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN.name(), conf);
        PROPERTIES_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.name(), conf);
        TYPE_HINTS_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.TYPE_HINTS_BIN.name(), conf);
        COUNTER_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.COUNTER_BIN.name(), conf);
        ID_TYPE_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.ID_TYPE_BIN.name(), conf);
        GRAPH_VARIABLES_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.GRAPH_VARIABLES_BIN.name(), conf);
        IN_EDGES_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.IN_EDGES_BIN.name(), conf);
        OUT_EDGES_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.OUT_EDGES_BIN.name(), conf);
        EDGE_CACHE_DISABLED_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.EDGE_CACHE_DISABLED_BIN.name(), conf);
        RELATIONAL_VERTEX_TYPE_HINT_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.RELATIONAL_VERTEX_TYPE_HINT_BIN.name(), conf);
        SUPERNODES_IN_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.SUPERNODES_IN.name(), conf);
        SUPERNODES_OUT_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.SUPERNODES_OUT.name(), conf);
        SUPERNODE_EDGE_PROPERTIES_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.SUPERNODE_EDGE_PROPERTIES_BIN.name(), conf);
        LABEL_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.LABEL_BIN.name(), conf);
        USER_KEY_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.USER_KEY_BIN.name(), conf);
        TTL_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.TTL_BIN.name(), conf);
        USAGE_STATS_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.USAGE_STATS_BIN.name(), conf);
        EDGE_DATA_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.EDGE_DATA_BIN.name(), conf);
        BL_ROW_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.BL_ROW_BIN.name(), conf);
        BL_FILE_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.BL_FILE_BIN.name(), conf);
        BULK_LOAD_RECOVERY_BIN = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.BL_RECOVERY_BIN.name(), conf);

        AEROSPIKE_BATCH_READ_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_BATCH_READ_SIZE, conf);
        AEROSPIKE_BATCH_THRESHOLD = this.client.getNodes().length *
                ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_BATCH_PER_NODE_THRESHOLD, conf);
        FIREFLY_READ_THROUGH_CACHE_WEIGHT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT, conf);
        PHAT_EDGE_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PHAT_EDGE_SIZE, conf);
        MOVEMENT_BARRIER_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MOVEMENT_BARRIER_SIZE, conf);

        PROPERTY_ID_BUFFER_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PROPERTY_ID_BUFFER_SIZE, conf);
        VERTEX_ID_BUFFER_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.VERTEX_ID_BUFFER_SIZE, conf);
        EDGE_ID_BUFFER_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE, conf);

        MERGE_EDGE_TTL = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MERGE_EDGE_TTL, conf);
        MERGE_EDGE_EVAL_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MERGE_EDGE_EVAL_TIMEOUT, conf);
        MERGE_EDGE_POLL_INTERVAL = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MERGE_EDGE_POLL_INTERVAL, conf);
        MERGE_EDGE_STARVATION_PROTECTION = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.MERGE_EDGE_STARVATION_PROTECTION, conf);

        USAGE_STATS_UPDATE_INTERVAL = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.USAGE_STATS_UPDATE_INTERVAL, conf);
        WARMUP_MODE = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.WARMUP_MODE, conf);
        PROMETHEUS_RENAME_ENABLED = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.PROMETHEUS_RENAME, conf);

        QUERY_IMPL = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.QUERY_IMPL, conf);

        bulkLoaderFlag = ConfigurationHelper.getOrDefaultBool(BULK_LOADER_FLAG, conf);
        bulkLoaderInitializerFlag = ConfigurationHelper.getOrDefaultBool(BULK_LOADER_INITIALIZER_FLAG, conf);
        olapEnabledFlag = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.OLAP_ENABLED, conf);

        idFactory = new FireflyIdFactory(this);

        MRT_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MRT_TIMEOUT, conf);
        MRT_ENABLED = ConfigurationHelper.getOrDefaultBool(MRT_ENABLED_FLAG, conf);
        if (MRT_ENABLED) {
            validateMrtSupport();
        }

        vertexPropertyBins.add(VERTEX_PROPERTY_NAME_TO_VALUE_BIN); // 2
        vertexPropertyBins.add(VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN); // 3
        vertexNonPropertyBins.add(RELATIONAL_VERTEX_TYPE_HINT_BIN); // 4
        vertexNonPropertyBins.add(EDGE_CACHE_DISABLED_BIN); // 6
        vertexNonPropertyBins.add(IN_EDGES_BIN); // 7
        vertexNonPropertyBins.add(OUT_EDGES_BIN); // 8
        vertexNonPropertyBins.add(PROPERTIES_BIN); // 9 --> These are not included in vertex property bins since they are not property key mapped.
        vertexNonPropertyBins.add(TYPE_HINTS_BIN); // 10 --> These are not included in vertex property bins since they are not property key mapped.
        vertexNonPropertyBins.add(ID_TYPE_BIN); // 12
        vertexNonPropertyBins.add(USER_KEY_BIN); // 13
        vertexNonPropertyBins.add(LABEL_BIN); // 14
        vertexPropertyBins.add(VERTEX_PROPERTY_NAME_TO_ID_BIN);// 17

        // Set Edge cache size
        final long onRecordIdMaxLimit = getRecordIdLimitFromAerospike(.9);
        ConfigurationHelper.setOnRecordIdLimit(onRecordIdMaxLimit);
        int onRecordIdLimit;
        try {
            onRecordIdLimit = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, conf);
            if (MRT_ENABLED && onRecordIdLimit > 1023) {
                LOG.warn("The provided value for '{}' could not be used and has been instead set to the maximum allowed value of 1023 for when '{}' is set as true.", ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, MRT_ENABLED_FLAG);
            }
        } catch (final ConfigurationRuntimeException ignored) {
            // If it was not manually configured, dynamically adjust it relative to the max-record-size configuration of Aerospike
            final long onRecordIdDefaultLimit = getRecordIdLimitFromAerospike(.45);
            onRecordIdLimit = onRecordIdDefaultLimit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) onRecordIdDefaultLimit;
        }

        // MRT operation can handle only 4096 records, so for Vertex drop we can touch no more than 1023 edges
        // (1023*2*2+1) < 4096
        if (MRT_ENABLED && onRecordIdLimit > 1023) {
            onRecordIdLimit = 1023;
        }
        LOG.info("{} configured to {}.", ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, onRecordIdLimit);
        ON_RECORD_ID_LIMIT = onRecordIdLimit;
    }

    private long getRecordIdLimitFromAerospike(final double fillPercentage) {
        final double maxRecordSizeBytes = InfoOps.getMaxRecordSizeBytes(this.client, this.namespace);
        final double edgeCacheIdSizeBytes = 36;
        final double numberOfCaches = 2;
        return (long) (((maxRecordSizeBytes * fillPercentage) / edgeCacheIdSizeBytes) / numberOfCaches);
    }

    /**
     * Connect to an Aerospike cluster while specifying a client.
     *
     * @param conf       Apache Configuration
     * @param client     Aerospike Client
     * @param eventLoops Aerospike Event Loops
     * @return Database connection handle
     */
    public static AerospikeConnection connect(final FireflyConfiguration conf,
                                              final AerospikeClient client,
                                              final EventLoops eventLoops,
                                              final ExecutorService threadedReadExecutor) {
        return new AerospikeConnection(conf, client, eventLoops, threadedReadExecutor);
    }


    /**
     * Connect to an Aerospike cluster.
     *
     * @param conf Apache Configuration
     * @return Database connection handle
     */
    public static AerospikeConnection connect(final Configuration conf) {
        final FireflyConfiguration fireflyConfig = FireflyConfiguration.fromConfiguration(conf);
        final AerospikeClientProvider provider = DefaultAerospikeClientProvider.connect(fireflyConfig);
        return connect(fireflyConfig, provider.getAerospikeClient(fireflyConfig), provider.getEventLoops(fireflyConfig),
                provider.getThreadedExecutorService(fireflyConfig));
    }


    /**
     * Aerospike put with exception handling
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     */
    public void checkedPut(final WritePolicy policy, final Key key, final Bin... bins) {
        final WritePolicy writePolicy;
        if (policy == null) {
            writePolicy = new WritePolicy();
        } else {
            writePolicy = policy;
        }
        configureWritePolicy(writePolicy);
        try {
            client.put(writePolicy, key, bins);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    /**
     * Get scan hit counter
     *
     * @return Scan hit counter
     */
    public ScanHitCounter getScanHitCounter() {
        if (scanHitCounterThreadLocal.get() == null)
            scanHitCounterThreadLocal.set(new ScanHitCounter());
        return scanHitCounterThreadLocal.get();
    }

    public void resetScanHitCounter() {
        this.scanHitCounterThreadLocal.set(null);
    }

    /**
     * Get the idFactory instance
     *
     * @return FireflyIdFactory
     */
    public FireflyIdFactory getIdFactory() {
        return idFactory;
    }

    public GraphMetadata getDataModelMetadata() {
        final Key k = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        final Policy policy = new Policy();
        policy.sendKey = false;
        Record dataModelRec = read(k, policy, null);
        return new GraphMetadata(dataModelRec);
    }

    public void setGraphMetadata(final String name, final String version) {
        final Bin dataModelNameBin = new Bin(DATA_MODEL_NAME, name);
        final Operation writeName = Operation.put(dataModelNameBin);
        final Bin dataModelVersionBin = new Bin(DATA_MODEL_VER, version);
        final Operation writeVersion = Operation.put(dataModelVersionBin);

        final Key k = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        this.writeOperate(null, k, writeName, writeVersion);
    }

    public synchronized void checkConfigurationCompatibility(final Configuration config) {
        final Key key = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        final Map<String, String> existingConfig = getDataModelMetadata().getExistingImmutableConfigs();

        if (config.containsKey(ConfigurationHelper.Keys.WARMUP_MODE.toLowerCase())
                && config.getBoolean(ConfigurationHelper.Keys.WARMUP_MODE.toLowerCase())) {
            // Warmup mode is enabled so we don't need to check the configuration,
            // it's a compatible copy intended to be slightly different to not interfere.
            // Note if we check it the summary mismatch will fail below, but we don't want
            // it to write to the summary.
            return;
        }

        if (existingConfig == null) {
            // This is a fresh graph so write the immutable configurations
            final Map<String, String> configurations = new HashMap<>();
            for (final String configKey : IMMUTABLE_CONFIG_KEYS) {
                configurations.put(configKey, getOrDefaultString(configKey, config));
            }
            final Bin configBin = new Bin(DATA_MODEL_CONF, configurations);
            final Operation writeConfig = Operation.put(configBin);
            this.writeOperate(null, key, writeConfig);
        } else {
            final List<Operation> newImmutableConfigs = new ArrayList<>();
            for (final String configKey : IMMUTABLE_CONFIG_KEYS) {
                if (existingConfig.containsKey(configKey)) {
                    if (!existingConfig.get(configKey).equalsIgnoreCase(getOrDefaultString(configKey, config))) {
                        final String error = "Cannot start Aerospike Graph Service due to existing immutable graph " +
                                "configuration '" + configKey + "' with value '" + existingConfig.get(configKey) +
                                "' mismatching provided configuration value '" + getOrDefaultString(configKey, config) + "'.";
                        LOG.error(error);
                        throw new IllegalArgumentException(error);
                    }
                } else {
                    // This is a newer version of Firefly with additional immutable configurations that we need to record.
                    final Operation addImmutableConfig = MapOperation.put(MapPolicy.Default, DATA_MODEL_CONF,
                            Value.get(configKey), Value.get(getOrDefaultString(configKey, config)));
                    newImmutableConfigs.add(addImmutableConfig);
                }
            }
            if (!newImmutableConfigs.isEmpty()) {
                this.writeOperate(null, key, newImmutableConfigs.toArray(new Operation[0]));
            }
        }
    }

    public static class GraphMetadata {
        public final Record metadataRecord;

        public GraphMetadata(final Record metadata) {
            this.metadataRecord = metadata;
        }

        public ComparableVersion getDataModelVersion() {
            if (this.metadataRecord == null) {
                return null;
            } else {
                return new ComparableVersion(this.metadataRecord.getString(DATA_MODEL_VER));
            }
        }

        public String getDataModelName() {
            if (this.metadataRecord == null) {
                return null;
            } else {
                return this.metadataRecord.getString(DATA_MODEL_NAME);
            }
        }

        public Map<String, String> getExistingImmutableConfigs() {
            if (this.metadataRecord == null) {
                return null;
            } else {
                final Map<?, ?> fixedConfig = this.metadataRecord.getMap(DATA_MODEL_CONF);
                if (fixedConfig == null) {
                    return null;
                } else {
                    return (Map<String, String>) fixedConfig;
                }
            }
        }
    }

    public static class InfoOps {
        public static class Keys {
            public static final String SET = "set";
            public static final String SETS = "sets";
            public static final String NS = "ns";
            public static final String OBJECTS = "objects";
            public static final String SINDEX = "sindex";
            public static final String SINDEX_LIST = "sindex-list";
            public static final String FEATURE_KEY = "feature-key";
            public static final String INDEXNAME = "indexname";
            public static final String RESULT = "result";
            public static final String GET_CONFIG = "get-config:context=namespace;id=";
            public static final String GET_SERVICE = "get-config:context=service";
            public static final String SHOW_ALL_QUERY = "query-show";
            public static final String QUERY_ABORT = "query-abort:trid=";
        }

        private static final String MAX_RECORD_SIZE = "max-record-size";
        private static final String DEFAULT_TTL = "default-ttl";
        private static final String STORAGE_ENGINE = "storage-engine";
        private static final String WRITE_BLOCK_SIZE = "storage-engine.write-block-size";
        private static final String STORAGE_ENGINE_PMEM = "pmem";
        private static final String STORAGE_ENGINE_MEMORY = "memory";
        private static final String QUERY_STATUS = "status";
        private static final String QUERY_TRID = "trid";
        private static final String QUERY_ABORT_RESULT = "result";
        private static final String QUERY_ABORT_SUCCESS = "OK";
        private static final String QUERY_ABORT_TRID_INACTIVE = "trid-not-active";
        private static final String QUERY_THREADS_LIMIT = "query-threads-limit";
        private static final String SINGLE_QUERY_THREADS = "single-query-threads";

        //Parse the whole infoResponse and return it as a List of Maps
        public static List<Map<String, String>> parseRaw(final String infoResponse) {
            final List<Map<String, String>> results = new ArrayList<>();
            Arrays.stream(infoResponse.split(";"))
                    .map(str -> str.split(":"))
                    .forEach(strAry -> {
                        Map<String, String> data = new TreeMap<>();
                        Arrays.stream(strAry).forEach(entryStr -> {
                            if (entryStr.isEmpty())
                                return;
                            if (entryStr.contains("="))
                                data.put(entryStr.split("=")[0], entryStr.split("=")[1]);
                            else
                                data.put(Keys.RESULT, entryStr);
                        });
                        if (data.size() > 0)
                            results.add(data);
                    });
            return results;
        }

        public static Map<String, Map<String, String>> parseBySet(final String infoResponse, final String namespace) {
            final Map<String, Map<String, String>> results = new TreeMap<>();
            Arrays.stream(infoResponse.split(";"))
                    .filter(str -> str.startsWith(Keys.NS + "=" + namespace))
                    .map(str -> str.split(":"))
                    .forEach(strAry -> {
                        Map<String, String> data = new TreeMap<>();
                        Arrays.stream(strAry).forEach(kvStr -> {
                            data.put(kvStr.split("=")[0], kvStr.split("=")[1]);
                        });
                        results.put(data.get(Keys.SET), data);
                    });
            return results;
        }

        /**
         * Return list of existing indices in a list of map entries.
         *
         * @param db AerospikeConnection
         * @return List of existing indices in a list of map entries.
         * First item of map entry is index
         * Second item of map entry is set the index belongs to
         */
        public static List<Map.Entry<String, String>> listExistingIndexes(final AerospikeConnection db) {
            final IAerospikeClient client = db.client;
            final String namespace = db.namespace;
            // Using client.getNodes()[0] is okay here since indexes exist across all nodes.
            LOG.debug("Info.request: {}", Keys.SINDEX);
            try {
                final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], Keys.SINDEX);
                return parseRaw(infoResponse).stream()
                        .filter(m -> m.get(Keys.NS).equals(namespace))
                        .map(m -> (Map.Entry<String, String>)
                                new AbstractMap.SimpleEntry(m.get(Keys.INDEXNAME), m.get(Keys.SET)))
                        .collect(Collectors.toList());
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        public static List<String> createSetIndex(final AerospikeConnection db, final String set) {
            final IAerospikeClient client = db.client;
            final String namespace = db.namespace;
            final String command = "set-config:context=namespace;id=" + namespace + ";set=" + set + ";enable-index=true";
            final List<String> results = new ArrayList<>();
            try {
                for (final Node node : client.getNodes()) {
                    results.add(Info.request(new InfoPolicy(), node, command));
                }
                return results;
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        /**
         * Return list of usable indices in a list of map entries.
         *
         * @param db        AerospikeConnnection.
         * @param namespace Namespace.
         * @return List of existing indices in a list of map entries.
         * First item of map entry is index
         * Second item of map entry is set the index belongs to
         */
        public static List<String> listUsableIndexes(final AerospikeConnection db, final String namespace) {
            final List<Set<String>> indexSets = new ArrayList<>();
            try {
                for (final Node node : db.client.getNodes()) {
                    LOG.debug("Info.request: {}", Keys.SINDEX);
                    final String infoResponse = Info.request(new InfoPolicy(), node, Keys.SINDEX);
                    final List<Map.Entry<String, String>> raw = parseRaw(infoResponse).stream()
                            .filter(m -> m.get(Keys.NS).equals(namespace))
                            .filter(m -> m.get("state").equals("RW"))
                            .map(m -> (Map.Entry<String, String>)
                                    new AbstractMap.SimpleEntry(m.get(Keys.INDEXNAME), m.get(Keys.SET)))
                            .collect(Collectors.toList());
                    indexSets.add(raw.stream().map(Map.Entry::getKey).filter(s ->
                                    s.startsWith(db.getVpIndexPrefix()) ||
                                            s.startsWith(db.getEpIndexPrefix()) ||
                                            db.V_LABEL_INDEX_NAME.equals(s) ||
                                            db.E_LABEL_INDEX_NAME.equals(s)).
                            collect(Collectors.toSet()));
                }

                final List<String> indexList = new ArrayList<>();
                for (final Set<String> set : indexSets) {
                    if (indexList.isEmpty()) {
                        indexList.addAll(set);
                    } else {
                        indexList.retainAll(set);
                    }
                }
                return indexList;
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        public static int getMaxParallelSindexes(final AerospikeConnection db, final String namespace) {
            final String namespaceConfigKey = Keys.GET_CONFIG + namespace;
            final String serviceConfigKey = Keys.GET_SERVICE;
            int maxParallelSindexes = Integer.MAX_VALUE;

            try {
                final IAerospikeClient client = db.client;
                for (final Node node : client.getNodes()) {
                    LOG.debug("Info.request: {}", namespaceConfigKey);
                    final String singleQueryThreads = Info.request(new InfoPolicy(), node, namespaceConfigKey);
                    List<Map<String, String>> listOfConfigs = parseRaw(singleQueryThreads);
                    int singleQueryThreadsValue = 4; // https://aerospike.com/docs/server/reference/configuration#namespace__single-query-threads
                    for (final Map<String, String> config : listOfConfigs) {
                        if (config.containsKey(SINGLE_QUERY_THREADS))
                            singleQueryThreadsValue = Integer.parseInt(config.get(SINGLE_QUERY_THREADS));
                    }
                    LOG.debug("Info.request: {}", namespaceConfigKey);
                    final String queryThreadsLimit = Info.request(new InfoPolicy(), node, serviceConfigKey);
                    listOfConfigs = parseRaw(queryThreadsLimit);
                    int queryThreadsLimitValue = 128; // https://aerospike.com/docs/server/reference/configuration#service__query-threads-limit
                    for (final Map<String, String> config : listOfConfigs) {
                        if (config.containsKey(QUERY_THREADS_LIMIT))
                            queryThreadsLimitValue = Integer.parseInt(config.get(QUERY_THREADS_LIMIT));
                    }

                    maxParallelSindexes = Math.min(maxParallelSindexes, queryThreadsLimitValue / singleQueryThreadsValue);
                }
                System.out.println("Max parallel sindexes: " + maxParallelSindexes);
                return maxParallelSindexes;
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        public static List<String> getIndexStatuses(final AerospikeConnection db, final String indexName) {
            final String infoQueryFormat = "sindex/%s/%s"; // "sindex/<namespace>/<index name>
            final String infoVar = String.format(infoQueryFormat, db.namespace, indexName);
            final InfoPolicy policy = new InfoPolicy();
            final List<String> responses = new ArrayList<>();
            LOG.debug("Info.request: {}", infoVar);
            for (final Node node : db.client.getNodes()) {
                try {
                    final String infoResponse = Info.request(policy, node, infoVar);
                    responses.add(infoResponse);
                } catch (final AerospikeException e) {
                    throw fromAerospikeException(e);
                }
            }
            return responses;
        }

        /**
         * Get whether TTL is enabled in Aerospike or not.
         *
         * @param client    client.
         * @param namespace Namespace.
         * @return True if enabled on any node, false otherwise.
         */
        public static boolean getIsAerospikeTTLEnabled(final AerospikeClient client, final String namespace) {
            final String requestKey = Keys.GET_CONFIG + namespace;

            try {
                final Node[] nodes = client.getNodes();
                for (final Node node : nodes) {
                    LOG.debug("Info.request: {}", requestKey);
                    final String infoResponse = Info.request(new InfoPolicy(), node, requestKey);
                    final List<Map<String, String>> listOfConfigs = parseRaw(infoResponse);
                    for (final Map<String, String> config : listOfConfigs) {
                        if (config.containsKey(DEFAULT_TTL)) {
                            if (!config.get(DEFAULT_TTL).equals("0")) {
                                LOG.error("One or more Aerospike node has default-ttl set to non-zero value: " + config.get(DEFAULT_TTL) +
                                        " in the namespace '" + namespace + "'. Please set default-ttl to 0 in all Aerospike " +
                                        "configuration files under the namespace '" + namespace + "'.");
                                return true;
                            }
                        }
                    }
                }
                return false;
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        /**
         * Return the max-record-size configured on Aerospike. If Aerospike is in a cluster, returns the value for the
         * node with the smallest max-record-size.
         *
         * @param client    client.
         * @param namespace Namespace.
         * @return The max-record-size.
         */
        public static long getMaxRecordSizeBytes(final IAerospikeClient client, final String namespace) {
            final String requestKey = Keys.GET_CONFIG + namespace;
            long maxRecordSize = Long.MAX_VALUE;

            try {
                final Node[] nodes = client.getNodes();
                for (final Node node : nodes) {
                    LOG.debug("Info.request: {}", requestKey);
                    final String infoResponse = Info.request(new InfoPolicy(), node, requestKey);
                    final List<Map<String, String>> listOfConfigs = parseRaw(infoResponse);
                    final Map<String, String> relevantConfigs = new HashMap<>();
                    for (final Map<String, String> config : listOfConfigs) {
                        if (config.containsKey(MAX_RECORD_SIZE)) {
                            relevantConfigs.put(MAX_RECORD_SIZE, config.get(MAX_RECORD_SIZE));
                        }
                        if (config.containsKey(WRITE_BLOCK_SIZE)) {
                            relevantConfigs.put(WRITE_BLOCK_SIZE, config.get(WRITE_BLOCK_SIZE));
                        }
                        if (config.containsKey(STORAGE_ENGINE)) {
                            relevantConfigs.put(STORAGE_ENGINE, config.get(STORAGE_ENGINE));
                        }
                    }

                    if (relevantConfigs.containsKey(MAX_RECORD_SIZE) && Long.parseLong(relevantConfigs.get(MAX_RECORD_SIZE)) != 0) {
                        maxRecordSize = Long.min(maxRecordSize, Long.parseLong(relevantConfigs.get(MAX_RECORD_SIZE)));
                    } else if (relevantConfigs.containsKey(WRITE_BLOCK_SIZE)) {
                        // When max-record-size is 0, it means that it was not set and will use the value of write-block-size instead
                        maxRecordSize = Long.min(maxRecordSize, Long.parseLong(relevantConfigs.get(WRITE_BLOCK_SIZE)));
                    } else if (STORAGE_ENGINE_PMEM.equals(relevantConfigs.get(STORAGE_ENGINE)) ||
                            STORAGE_ENGINE_MEMORY.equals(relevantConfigs.get(STORAGE_ENGINE))) {
                        // These storage types are hard-coded to 8MiB
                        LOG.info("Storage type \"" + relevantConfigs.get(STORAGE_ENGINE) + "\" detected. Using maximum record size of 8MiB.");
                        maxRecordSize = Long.min(maxRecordSize, 8388608);
                    } else {
                        LOG.warn("Unexpected failure to determine maximum record size based on Aerospike configuration. Falling back to maximum record size of 1MiB.");
                        maxRecordSize = Long.min(maxRecordSize, 1048576);
                    }
                }

                return maxRecordSize;
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        public static Map<String, Integer> abortAllQueries(final IAerospikeClient client, final String namespace) {
            final String requestKey = Keys.SHOW_ALL_QUERY;

            final Set<String> activeTrids = new HashSet<>();

            try {
                final Node[] nodes = client.getNodes();
                for (final Node node : nodes) {
                    LOG.debug("Info.request: {}", requestKey);
                    final String infoResponse = Info.request(new InfoPolicy(), node, requestKey);
                    final List<Map<String, String>> listOfQueries = parseRaw(infoResponse).stream()
                            .filter(m -> m.get(Keys.NS).equals(namespace))
                            .collect(Collectors.toList());
                    for (final Map<String, String> query : listOfQueries) {
                        if (!query.containsKey(QUERY_STATUS)) {
                            LOG.error("Unexpected failure to get a query's status when aborting all queries. Please contact support if an ongoing query persists.");
                            continue;
                        }
                        if (query.get(QUERY_STATUS).contains(("active"))) {
                            if (!query.containsKey(QUERY_TRID)) {
                                LOG.error("Unexpected failure to get an active query's trid when aborting all queries. Please contact support if an ongoing query persists.");
                                continue;
                            }
                            activeTrids.add(query.get(QUERY_TRID));
                        }
                    }
                }
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }

            final Map<String, Integer> resultMap = new HashMap<>();
            LOG.info("Found {} active queries to abort.", activeTrids.size());
            resultMap.put("found", activeTrids.size());
            int successfulAborts = 0;
            for (final String trid : activeTrids) {
                if (abortQuery(client, trid)) {
                    successfulAborts++;
                }
            }
            LOG.info("{} active queries were successfully aborted.", successfulAborts);
            resultMap.put("aborted", successfulAborts);
            return resultMap;
        }

        public static boolean abortQuery(final IAerospikeClient client, final String trid) {
            final String requestKey = Keys.QUERY_ABORT + trid;

            try {
                boolean success = false;
                String lastAbortResult = null;
                final Node[] nodes = client.getNodes();
                for (final Node node : nodes) {
                    LOG.debug("Info.request: {}", requestKey);
                    final String infoResponse = Info.request(new InfoPolicy(), node, requestKey);
                    final List<Map<String, String>> queryAbortResponses = parseRaw(infoResponse);
                    for (final Map<String, String> abortResponse : queryAbortResponses) {
                        if (abortResponse.containsKey(QUERY_ABORT_RESULT)) {
                            final String abortResult = abortResponse.get(QUERY_ABORT_RESULT);
                            if (QUERY_ABORT_SUCCESS.equals(abortResult) || QUERY_ABORT_TRID_INACTIVE.equals(abortResult)) {
                                success = true;
                            } else {
                                if (!abortResult.equals(lastAbortResult)) {
                                    LOG.error("Aborting query with trid {} failed with response: {}", trid, abortResult);
                                    lastAbortResult = abortResult;
                                }
                            }
                        }
                    }
                }
                if (!success) {
                    LOG.error("Failed to abort query with trid {}. Please contact support if an ongoing query persists.", trid);
                }
                return success;
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        /**
         * Is the first connected Aerospike instance "Enterprise Edition"
         *
         * @param db AerospikeConnection instance
         * @return enterprise or not
         */
        public static boolean isEnterprise(final AerospikeConnection db) {
            final IAerospikeClient client = db.client;
            // Using client.getNodes()[0] is okay here since if one is enterprise, the entire cluster is.
            LOG.debug("Info.request: {}", Keys.FEATURE_KEY);
            try {
                final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], Keys.FEATURE_KEY);
                return (infoResponse != null && !infoResponse.isEmpty());
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        public static String getClusterName(final AerospikeConnection db) {
            final IAerospikeClient client = db.client;
            LOG.debug("Info.request: get-config");
            try {
                final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], "get-config");
                final String[] delimitedResponse = infoResponse.split(";");
                for (final String s : delimitedResponse) {
                    if (s.startsWith("cluster-name=")) {
                        if ("null".equals(s.split("=")[1])) {
                            return "";
                        }
                        return s.split("=")[1];
                    }
                }
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
            throw new IllegalStateException("Could not find cluster-name in get-config response.");
        }

        /**
         * Get a list of all the Sets in a namespace that have a number of records > 0
         *
         * @param db AerospikeConnection instance
         * @return Set of namespaces
         */
        public static Set<String> getNonEmptySetList(final AerospikeConnection db) {
            final IAerospikeClient client = db.client;
            final String namespace = db.namespace;
            final Set<String> allSets = new HashSet<>();

            // Need to loop all nodes here in case one of the sets only has data on a single node.
            try {
                for (final Node node : client.getNodes()) {
                    LOG.debug("Info.request: {}", Keys.SETS);
                    final String infoResponse = Info.request(new InfoPolicy(), node, Keys.SETS);
                    allSets.addAll(parseBySet(infoResponse, namespace).entrySet().stream().filter(entry -> {
                                Map<String, String> map = entry.getValue();
                                return Integer.parseInt(map.get(Keys.OBJECTS)) > 0;
                            })
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new))
                            .keySet());
                }
                return allSets;
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        public static String getDatabaseVersionString(final AerospikeConnection db) {
            final IAerospikeClient client = db.client;
            final StringBuilder versionString = new StringBuilder();
            try {
                for (final Node node : client.getNodes()) {
                    LOG.debug("Info.request: build");
                    final String response = Info.request(null, node, "build");
                    if (!versionString.toString().isEmpty()) {
                        versionString.append(",");
                    }
                    versionString.append(node.getAddress().getHostName()).append(":").append(response);
                }
                return versionString.toString();
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        public static String singleNodeInfoRequest(final AerospikeConnection db, final String infoVar) {
            LOG.debug("Info.request: {}", infoVar);
            final Node[] nodes = db.client.getNodes();
            if (nodes.length == 0) {
                throw new AerospikeGraphException(GraphError.NO_ACTIVE_NODES);
            }
            try {
                return Info.request(null, nodes[0], infoVar);
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }
    }

    public static final Map<Class<? extends Serializable>, Long> SUPPORTED_VALUE_TYPES = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(byte[].class, 4L);
        put(Byte[].class, 4L);
        put(String.class, 5L);
        put(Boolean.class, 6L);
        put(ArrayList.class, 7L);
    }};
    public static final Map<Long, Class<? extends Serializable>> SUPPORTED_TYPE_VALUES = new HashMap<>() {{
        put(1L, Long.class);
        put(2L, Integer.class);
        put(3L, Double.class);
        put(4L, byte[].class);
        put(5L, String.class);
        put(6L, Boolean.class);
        put(7L, ArrayList.class);
    }};
    public static final Set<Class<? extends Serializable>> SUPPORTED_ARR_TYPES = new HashSet<>() {{
        add(boolean[].class);
        add(Boolean[].class);
        add(double[].class);
        add(Double[].class);
        add(int[].class);
        add(Integer[].class);
        add(String[].class);
        add(long[].class);
        add(Long[].class);
    }};

    static AtomicLong readMetric = new AtomicLong(0);
    static AtomicLong writeMetric = new AtomicLong(0);

    /**
     * Return the name of the set associated with the FireflyElement class
     *
     * @param type Firefly Element class
     * @return name of the set
     */
    public String setFromElementType(final Class<? extends FireflyElement> type) {
        if (FireflyEdge.class.isAssignableFrom(type))
            return EDGE_AERO_SET;
        else if (FireflyVertex.class.isAssignableFrom(type))
            return VERTEX_AERO_SET;
        else if (FireflyVertexProperty.class.isAssignableFrom(type)) {
            return VERTEX_AERO_SET;
        }
        throw new UnsupportedOperationException("Element not supported " + type.getName());
    }

    /**
     * If the value parameter is scalar, return the numeric id of the on disk type if value is an Integer - else null.
     * If the value parameter is an ArrayList, return an ArrayList containing the indices at which the values within the
     * parameter ArrayList is an Integer. Returns null if the ArrayList contained no Integer values.
     *
     * @param value Object to get type hint ID of
     * @return Type hint value or null
     */
    public static Object getTypeHintOf(final Object value) {
        final Class clazz = value.getClass();
        if (!SUPPORTED_VALUE_TYPES.containsKey(clazz)) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        } else if (SUPPORTED_VALUE_TYPES.get(clazz).equals(SUPPORTED_VALUE_TYPES.get(ArrayList.class))) {
            // Values within a list for our supported types are stored on disk as expected except for Integers which get
            // stored as a Long. We need to keep track of which indexes within the list were inputted as Integers to
            // properly cast them back upon a read.
            final ArrayList<Long> integerIndices = new ArrayList<>();
            final ArrayList<?> valueList = (ArrayList<?>) value;
            for (int i = 0; i < valueList.size(); i++) {
                final Object valueInList = valueList.get(i);
                if (valueInList != null) {
                    final Class valueClass = valueInList.getClass();
                    if (!SUPPORTED_VALUE_TYPES.containsKey(valueClass) ||
                            SUPPORTED_VALUE_TYPES.get(valueClass).equals(SUPPORTED_VALUE_TYPES.get(ArrayList.class))) {
                        throw new IllegalArgumentException(valueClass.getName()
                                + " within a List is not a supported value type");
                    }
                    if (valueList.get(i).getClass().equals(Integer.class)) {
                        integerIndices.add((long) i);
                    }
                }
            }
            return integerIndices.isEmpty() ? null : integerIndices;
        }
        return Objects.equals(SUPPORTED_VALUE_TYPES.get(clazz), SUPPORTED_VALUE_TYPES.get(Integer.class)) ? SUPPORTED_VALUE_TYPES.get(Integer.class) : null;
    }

    /**
     * Create Indexes for Firefly
     */
    public void createGraphIndexes() {
        final boolean warmup_mode = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.WARMUP_MODE, conf);
        if (warmup_mode || VERTEX_AERO_SET.contains(WarmupUtil.getWarmupArenaName()))
            return;

        if (!shouldCreateIndexes())
            return;

        try {
            LOG.info("Creating graph indices.");
            List<String> existingIndexes =
                    InfoOps.listExistingIndexes(this).stream()
                            .map(Map.Entry::getKey).collect(Collectors.toList());

            // Blocking call for supernode indexes since graph doesn't function without.
            createIndex(existingIndexes, setFromElementType(FireflyEdge.class),
                    E_IN_INDEX_NAME, SUPERNODES_IN_BIN,
                    IndexType.STRING, IndexCollectionType.MAPVALUES);
            createIndex(existingIndexes, setFromElementType(FireflyEdge.class),
                    E_OUT_INDEX_NAME, SUPERNODES_OUT_BIN,
                    IndexType.STRING, IndexCollectionType.MAPVALUES);

            // Blocking call for ttl indexes since graph doesn't function without.
            if (TTL_ENABLED_FLAG) {
                createIndex(existingIndexes, setFromElementType(FireflyVertex.class),
                        TTL_VERTEX_INDEX_NAME, TTL_BIN,
                        IndexType.NUMERIC, IndexCollectionType.DEFAULT);
                createIndex(existingIndexes, setFromElementType(FireflyEdge.class),
                        TTL_EDGE_INDEX_NAME, TTL_BIN,
                        IndexType.NUMERIC, IndexCollectionType.MAPVALUES);
            }

            // Create set index on GRAPH_METADATA_SET for lock records.
            List<String> setIndex = AerospikeConnection.InfoOps.createSetIndex(this, GRAPH_METADATA_SET);
            for (final String index : setIndex) {
                if (!"ok".equals(index)) {
                    LOG.error("Error creating set index for metadata set: {}", index);
                }
            }
            setIndex = AerospikeConnection.InfoOps.createSetIndex(this, BULK_LOAD_RECOVERY_VERTEX_SET);
            for (final String index : setIndex) {
                if (!"ok".equals(index)) {
                    LOG.error("Error creating set index for metadata set: {}", index);
                }
            }
            setIndex = AerospikeConnection.InfoOps.createSetIndex(this, BULK_LOAD_RECOVERY_EDGE_SET);
            for (final String index : setIndex) {
                if (!"ok".equals(index)) {
                    LOG.error("Error creating set index for metadata set: {}", index);
                }
            }
            setIndex = AerospikeConnection.InfoOps.createSetIndex(this, BULK_LOAD_BAD_EDGE_SET);
            for (final String index : setIndex) {
                if (!"ok".equals(index)) {
                    LOG.error("Error creating set index for metadata set: {}", index);
                }
            }
            setIndex = AerospikeConnection.InfoOps.createSetIndex(this, BULK_LOAD_RECOVERY_SUPERNODE_SET);
            for (final String index : setIndex) {
                if (!"ok".equals(index)) {
                    LOG.error("Error creating set index for metadata set: {}", index);
                }
            }

            // Create label index in background.
            if (V_LABEL_INDEX_ENABLED_FLAG) {
                createIndexBackground(existingIndexes, setFromElementType(FireflyVertex.class),
                        V_LABEL_INDEX_NAME, LABEL_BIN, IndexType.STRING, IndexCollectionType.DEFAULT, false);
            }
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
        if (E_LABEL_INDEX_ENABLED_FLAG) {
            // TODO GRAPH-438: Edge indexes.
            throw new RuntimeException("Edge indexes are not currently supported.");
        }
    }

    public boolean shouldCreateIndexes() {
        if (bulkLoaderFlag) {
            return bulkLoaderInitializerFlag;
        } else {
            return !olapEnabledFlag;
        }
    }

    /**
     * Drop indices for Firefly
     */
    public void dropGraphIndices(final FireflyGraph graph) {
        LOG.debug("Dropping graph indices.");
        dropIndex(setFromElementType(FireflyVertex.class), V_LABEL_INDEX_NAME);
        dropIndex(setFromElementType(FireflyEdge.class), E_LABEL_INDEX_NAME);
        if (graph != null) {
            graph.fireflyIndexMetadata.getIndexesInProgress().forEach(index -> {
                if (index.startsWith(getVpIndexPrefix())) {
                    dropIndex(setFromElementType(FireflyVertex.class), index);
                } else if (index.startsWith(getEpIndexPrefix())) {
                    dropIndex(setFromElementType(FireflyEdge.class), index);
                }
            });
        }
    }

    public Map<String, Integer> abortQueries() {
        return InfoOps.abortAllQueries(this.client, this.namespace);
    }

    public int getNodeCount() {
        return this.client.getNodes().length;
    }

    /**
     * Get the namespace configured for this instance of FireflyGraph
     *
     * @return namespace name
     */
    public String getNamespace() {
        return this.namespace;
    }

    private static EventLoops initializeEventLoops(final EventLoopType eventLoopType,
                                                   final int numLoops, final int commandsPerEventLoop,
                                                   final int maxCommandsInQueue) {
        final EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.maxCommandsInProcess = commandsPerEventLoop;
        eventPolicy.maxCommandsInQueue = maxCommandsInQueue;
        switch (eventLoopType) {
            case DIRECT_NIO:
                return new NioEventLoops(eventPolicy, numLoops);
            case NETTY_NIO:
                final NioEventLoopGroup nioGroup = new NioEventLoopGroup(numLoops);
                return new NettyEventLoops(eventPolicy, nioGroup);
            case NETTY_EPOLL:
                final EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(numLoops);
                return new NettyEventLoops(eventPolicy, epollGroup);
            default:
                // This should never happen.
                throw new IllegalArgumentException("Unsupported event loop type: " + eventLoopType);
        }
    }

    /**
     * Perform an Aerospike read by Key
     *
     * @param key    Aerospike Key to read
     * @param policy Aerospike Policy to use
     * @return Aerospike Record
     */
    public Record read(final Key key, final Policy policy) {
        return read(key, policy, null);
    }

    /**
     * Perform an Aerospike read by Key
     *
     * @param key    Aerospike Key to read
     * @param policy Aerospike Policy to use
     * @return Aerospike Record
     */
    public Record read(final Key key, final Policy policy, final FireflyCache cache) {
        final Policy readPolicy;
        if (policy == null) {
            readPolicy = new Policy();
        } else {
            readPolicy = policy;
        }
        configureReadPolicy(readPolicy);
        try {
            return (cache != null) ? cache.read(readPolicy, key) : client.get(readPolicy, key);
        } catch (final AerospikeException e) {
            LOG.error("Error: AerospikeException in read {}", e.getMessage());
            throw fromAerospikeException(e);
        }
    }

    /**
     * Perform an Aerospike read by Key
     *
     * @param key        Aerospike Key to read
     * @param policy     Aerospike WritePolicy to use
     * @param operations Read operations
     * @return Aerospike Record
     */
    public Record read(final Key key, final WritePolicy policy, final Operation[] operations) {
        return read(key, policy, operations, null);
    }

    /**
     * Perform an Aerospike read by Key
     *
     * @param key        Aerospike Key to read
     * @param policy     Aerospike WritePolicy to use
     * @param operations Read operations
     * @return Aerospike Record
     */
    public Record read(final Key key, final WritePolicy policy, final Operation[] operations, final FireflyCache cache) {
        final WritePolicy writePolicy;
        if (policy == null) {
            writePolicy = new WritePolicy();
        } else {
            writePolicy = policy;
        }
        // Operations require a WritePolicy but we only use this function to read.
        configureReadPolicy(writePolicy);
        try {
            return (cache != null) ? cache.read(writePolicy, key, operations) : client.operate(writePolicy, key, operations);
        } catch (final AerospikeException e) {
            LOG.error("Error: AerospikeException in read {}", e.getMessage());
            throw fromAerospikeException(e);
        }
    }

    /**
     * Perform a batch Aerospike read for a group of keys
     *
     * @param keys       Array of Key to return records for
     * @param policy     BatchPolicy to use
     * @param operations Read operations
     * @return Array of Record
     */
    private Record[] batchRead(final Key[] keys, final BatchPolicy policy, final Operation[] operations, final FireflyCache cache) {
        final BatchPolicy batchPolicy = policy == null ? new BatchPolicy() : policy;
        batchPolicy.sendKey = false;
        configureReadPolicy(batchPolicy);
        try {
            return (cache != null) ? cache.read(keys, batchPolicy, operations) : client.get(batchPolicy, keys, operations);
        } catch (final AerospikeException e) {
            LOG.error("Error: AerospikeException in read {}", e.getMessage());
            throw fromAerospikeException(e);
        }
    }

    /**
     * Perform a batch Aerospike read for a group of keys
     *
     * @param keys   Array of Key to return records for
     * @param policy BatchPolicy to use
     * @return Array of Record
     */
    private Record[] batchRead(final Key[] keys, final BatchPolicy policy, final FireflyCache cache) {
        final BatchPolicy batchPolicy = policy == null ? new BatchPolicy() : policy;
        batchPolicy.sendKey = false;
        configureReadPolicy(batchPolicy);
        try {
            return (cache != null) ? cache.read(keys, batchPolicy) : client.get(batchPolicy, keys);
        } catch (final AerospikeException e) {
            LOG.error("Error: AerospikeException in read {}", e.getMessage());
            throw fromAerospikeException(e);
        }
    }

    public void readWithListener(final RecordListener listener, final Policy policy, final Key key) {
        final Policy readPolicy = policy == null ? new Policy() : policy;
        configureReadPolicy(readPolicy);
        try {
            this.client.get(this.eventLoops.next(), listener, policy, key);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    /**
     * Perform an Aerospike read by Key and bypass the transaction cache. Should only be called by the cache.
     *
     * @param key    Aerospike Key to read
     * @param policy Aerospike Policy to use
     * @return Aerospike Record
     */
    Record skipCacheRead(final Key key, final Policy policy) {
        final Policy readPolicy = policy == null ? new Policy() : policy;
        configureReadPolicy(readPolicy);
        try {
            return client.get(readPolicy, key);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    /**
     * Perform an Aerospike read by Key and bypass the transaction cache. Should only be called by the cache.
     *
     * @param key    Aerospike Key to read
     * @param policy Aerospike WritePolicy to use
     * @return Aerospike Record
     */
    Record skipCacheRead(final Key key, final WritePolicy policy, final Operation[] operations) {
        final WritePolicy writePolicy = policy == null ? new WritePolicy() : policy;
        configureReadPolicy(writePolicy);
        try {
            return client.operate(writePolicy, key, operations);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    /**
     * Perform a batch Aerospike read for a group of keys and bypass the transaction cache. Should only be called by
     * the cache.
     *
     * @param keys       Array of Key to return records for
     * @param policy     BatchPolicy to use
     * @param operations Read operations
     * @return Array of Record
     */
    Record[] skipCacheRead(final Key[] keys, final BatchPolicy policy, Operation[] operations) {
        final BatchPolicy batchPolicy = policy == null ? new BatchPolicy() : policy;
        batchPolicy.sendKey = false;
        configureReadPolicy(batchPolicy);
        try {
            return client.get(batchPolicy, keys, operations);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    /**
     * Perform a batch Aerospike read for a group of keys and bypass the transaction cache. Should only be called by
     * the cache.
     *
     * @param keys   Array of Key to return records for
     * @param policy BatchPolicy to use
     * @return Array of Record
     */
    Record[] skipCacheRead(final Key[] keys, final BatchPolicy policy) {
        final BatchPolicy batchPolicy = policy == null ? new BatchPolicy() : policy;
        batchPolicy.sendKey = false;
        configureReadPolicy(batchPolicy);
        try {
            return client.get(batchPolicy, keys);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    public Record[] dynamicBatchRead(final ReadInfo readInfo, final Key[] keys, final FireflyCache cache, final Operation... operations) {
        return dynamicBatchRead(keys, readInfo.expression, cache, operations);
    }

    public Record[] dynamicBatchRead(final Key[] keys, final Expression filterExp, final FireflyCache cache, final Operation... operations) {
        if (keys.length > this.AEROSPIKE_BATCH_THRESHOLD) {
            // Default batch read used by read.
            final BatchPolicy batchReadPolicy = new BatchPolicy();
            batchReadPolicy.filterExp = filterExp;
            return operations.length == 0 ? this.batchRead(keys, batchReadPolicy, cache) :
                    this.batchRead(keys, batchReadPolicy, operations, cache);
        } else {
            final WritePolicy policy = new WritePolicy();
            policy.filterExp = filterExp;
            final List<Future<Record>> futures = new ArrayList<>();
            for (final Key key : keys) {
                futures.add(CompletableFuture.supplyAsync(() -> {
                    if (operations.length == 0) {
                        return this.read(key, policy, cache);
                    } else {
                        return this.read(key, policy, operations, cache);
                    }
                }, threadedReadExecutor));
            }

            final Record[] results = new Record[keys.length];
            for (int i = 0; i < keys.length; i++) {
                try {
                    results[i] = futures.get(i).get();
                } catch (final InterruptedException e) {
                    // Should only happen if we are interrupted by TinkerPop.
                    LOG.error("Error: Exception in read {}", e.getMessage());
                    throw new TraversalInterruptedException();
                } catch (final ExecutionException e) {
                    // Should never happen.
                    LOG.error("Error: Exception in read {}", e.getMessage());
                    throw new RuntimeException(e);
                }
            }
            return results;
        }
    }

    public FireflyRecordSet query(final QueryPolicy policy, final Statement statement) {
        final QueryPolicy queryPolicy = policy == null ? new QueryPolicy() : policy;
        configureReadPolicy(queryPolicy);
        try {
            return new FireflyRecordSet(this.client.query(queryPolicy, statement));
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    /**
     * Write to Aerospike, notify the cache implementation
     *
     * @param key  Key to write Bins into
     * @param bins Data Bin(s) to write
     */
    public void write(final Key key, final Bin... bins) {
        Bin[] newBins;
        //@todo This is a temporary measure to pack the user key into a bin.
        //@todo Remove when sendKey works to recover the user key for hash constructed keys
        if (key.userKey.getObject() != null) {
            newBins = Arrays.copyOf(bins, bins.length + 1);
            newBins[bins.length] = new Bin(USER_KEY_BIN, Value.get(key.userKey.getObject()));
        } else {
            newBins = bins;
        }
        write(key, false, -1, newBins);
    }

    /**
     * Write to Aerospike, notify the cache implementation
     *
     * @param key  Key to write Bins into
     * @param bins Data Bin(s) to write
     */
    public void write(final Key key, final boolean writeOnly, final int generation, final Bin... bins) {
        Bin[] newBins;
        //@todo This is a temporary measure to pack the user key into a bin.
        //@todo Remove when sendKey works to recover the user key for hash constructed keys
        if (key.userKey.getObject() != null) {
            newBins = Arrays.copyOf(bins, bins.length + 1);
            newBins[bins.length] = new Bin(USER_KEY_BIN, Value.get(key.userKey.getObject()));
        } else {
            newBins = bins;
        }

        writeMetric.incrementAndGet();
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        if (writeOnly) {
            writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        }
        if (generation != -1) {
            // Set generation for write.
            writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            writePolicy.generation = generation;
        }
        final FireflyCache cache = transactionCache.get();
        final FireflyCache noPropsCache = emptyPropsTransactionCache.get();
        if (cache != null) {
            cache.write(writePolicy, key, newBins);
        } else {
            checkedPut(writePolicy, key, newBins);
        }
        if (noPropsCache != null) {
            noPropsCache.remove(key);
        }
    }

    /**
     * Determine of a key exists
     *
     * @param key Aerospike Key to check
     * @return Boolean key exists
     */
    public boolean exists(final Key key) {
        return client.exists(null, key);
    }

    /**
     * Determine of a key exists
     *
     * @param keys Aerospike Key to check
     * @return Boolean key exists
     */
    public boolean[] exists(final Key[] keys) {
        return exists(null, keys);
    }

    /**
     * Determine of a key exists
     *
     * @param keys       Aerospike Key to check
     * @param expression Aerospike expression to check on record.
     * @return Boolean key exists
     */
    public boolean[] exists(final Expression expression, final Key[] keys) {
        final List<Boolean> results = new ArrayList<>();
        final BatchPolicy batchPolicy = BatchPolicy.ReadDefault();
        batchPolicy.filterExp = expression;
        configureReadPolicy(batchPolicy);
        try {
            while (results.size() < keys.length) {
                final Key[] batchKeys = (keys.length - results.size() >= AEROSPIKE_BATCH_READ_SIZE) ?
                        Arrays.copyOfRange(keys, results.size(), results.size() + AEROSPIKE_BATCH_READ_SIZE - 1) :
                        Arrays.copyOfRange(keys, results.size(), keys.length);
                final boolean[] batchResults = client.exists(batchPolicy, batchKeys);
                for (boolean batchResult : batchResults) {
                    results.add(batchResult);
                }
            }
            final boolean[] resultArray = new boolean[results.size()];
            for (int i = 0; i < results.size(); i++) {
                resultArray[i] = results.get(i);
            }

            return resultArray;
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    /**
     * Delete by Key
     *
     * @param key Aerospike Key to delete
     * @return whether record existed on server before deletion
     */
    public boolean delete(final Key key, final Txn txn) {
        final FireflyCache cache = transactionCache.get();
        if (cache != null) {
            cache.invalidate(key);
        }
        final FireflyCache noPropsCache = emptyPropsTransactionCache.get();
        if (noPropsCache != null) {
            noPropsCache.invalidate(key);
        }
        final WritePolicy policy = new WritePolicy();
        configureWritePolicy(policy);
        policy.txn = txn;
        policy.durableDelete = txn != null;
        try {
            return client.delete(policy, key);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    /**
     * query if the connected Aerospike instance is licenced for Enterprise Edition
     *
     * @return connected Aerospike instance is Enterprise Edition
     */
    public boolean isEnterprise() {
        return InfoOps.isEnterprise(this);
    }

    /**
     * Get the current number of writes since startup on this instance of Firefly
     *
     * @return number of writes
     */
    public long getWriteMetric() {
        return writeMetric.get();
    }

    /**
     * Get the current number of reads since startup on this instance of Firefly
     *
     * @return number of reads
     */
    public long getReadMetric() {
        return readMetric.get();
    }

    /**
     * Return a named key-value from a map
     * read its associated type-hint and reconstruct the correct JVM type for the value
     *
     * @param aeroSet
     * @param fid
     * @param mapName
     * @param mapKey
     * @param <V>
     * @return
     */
    public <V> V readTypeHintedValueFromMap(final String aeroSet,
                                            final FireflyId fid,
                                            final String mapName,
                                            final Object mapKey,
                                            final String typeHintBin) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, fid);
        if (fireflyRecord == null || fireflyRecord.record() == null ||
                fireflyRecord.record().getMap(mapName) == null ||
                !fireflyRecord.record().getMap(mapName).containsKey(mapKey))
            return null;
        final Object value = fireflyRecord.record().getMap(mapName).get(mapKey);
        Object typeHint = null;
        if (fireflyRecord.record().getMap(typeHintBin) != null) {
            typeHint = fireflyRecord.record().getMap(typeHintBin).get(mapKey);
        }
        return (V) convertValuetoTypeUsingHint(value, typeHint);
    }

    public Object convertValuetoTypeUsingHint(final Object value, final Object typeHint) {
        if (typeHint == null) {
            return value;
        }
        if (typeHint instanceof ArrayList) {
            final ArrayList<Object> valueList = new ArrayList<>((ArrayList<Object>) value);
            final ArrayList<Long> integerIndices = (ArrayList<Long>) typeHint;
            for (final Long index : integerIndices) {
                final Object valueListValue = valueList.get(index.intValue());
                if (valueListValue instanceof Long) {
                    valueList.set(index.intValue(), ((Long) valueListValue).intValue());
                } else if (!(valueListValue instanceof Integer)) {
                    // This should never happen.
                    throw new IllegalStateException("A type hint for a list contains items that aren't int or long.");
                }
            }
            return valueList;
        }
        final Class clazz = SUPPORTED_TYPE_VALUES.get(typeHint);
        return (clazz == null) ? value : typeCast(clazz, value);
    }

    /**
     * remove a key-value from a map along with its type-hint
     *
     * @param aeroSet
     * @param fid
     * @param mapName
     * @param mapKey
     */
    public void removeTypeHintedValueFromMap(final String aeroSet,
                                             final FireflyId fid,
                                             final String mapName,
                                             final String mapKey,
                                             final String typeHintBin) {
        final Key key = getKey(this, aeroSet, fid);
        final Operation removeValue = MapOperation.removeByKey(mapName, Value.get(mapKey), MapReturnType.NONE);
        final Operation removeTypeHint = MapOperation.removeByKey(typeHintBin, Value.get(mapKey), MapReturnType.NONE);
        this.writeOperate(null, key, removeValue, removeTypeHint);
    }

    /**
     * Write a Graph Variable as a key-value pair into a Map on a Record.
     * Also store a type hint so it can be reconstructed as the correct type.
     * Default write policy to allow creation and overwriting of Graph Variables.
     *
     * @param aeroSet
     * @param fid
     * @param mapName
     * @param mapKey
     * @param value
     * @param typeHintBinName
     * @param <V>
     */
    public <V> void writeTypeHintedGraphVariable(final String aeroSet,
                                                 final FireflyId fid,
                                                 final String mapName,
                                                 final String mapKey,
                                                 final V value,
                                                 final String typeHintBinName) {
        writeTypeHintedValueToMapWithPolicy(aeroSet, fid, mapName, mapKey, value, typeHintBinName, null);
    }

    public <V> void writeTypeHintedValueToMapWithPolicy(final String aeroSet,
                                                        final FireflyId fid,
                                                        final String mapName,
                                                        final String mapKey,
                                                        final V value,
                                                        final String typeHintBinName,
                                                        final WritePolicy writePolicy,
                                                        final Bin... additionalBins) {
        final Key key = getKey(this, aeroSet, fid);
        final List<Operation> ops = new ArrayList<>();

        final Operation valueOp;
        final Operation typeHintOp;
        // Null value properties are not currently supported.
        // Expected behavior is to remove the existing property key if it exists when null value is written.
        if (value == null) {
            valueOp = MapOperation.removeByKey(mapName, Value.get(mapKey), MapReturnType.NONE);
            ops.add(valueOp);
            typeHintOp = MapOperation.removeByKey(typeHintBinName, Value.get(mapKey), MapReturnType.NONE);
            ops.add(typeHintOp);
        } else {
            final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            valueOp = MapOperation.put(policy, mapName, Value.get(mapKey), Value.get(value));
            ops.add(valueOp);
            final Object typeHint = getTypeHintOf(value);
            if (typeHint != null) {
                typeHintOp = MapOperation.put(policy, typeHintBinName, Value.get(mapKey),
                        Value.get(typeHint));
                ops.add(typeHintOp);
            }
        }
        final Expression idTypeExp = Exp.build(Exp.val(fid.getStorageTypeHint()));
        final Operation idTypeOp = ExpOperation.write(this.ID_TYPE_BIN, idTypeExp,
                ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL);
        ops.add(idTypeOp);
        for (final Bin bin : additionalBins) {
            ops.add(Operation.put(bin));
        }

        final Operation[] operations = ops.toArray(new Operation[0]);

        this.writeOperate(writePolicy, key, operations);
    }

    /**
     * Cast an on-disk storage type to its user type
     *
     * @param clazz
     * @param val
     * @return
     */
    public Object typeCast(final Class clazz, final Object val) {
        if (clazz.equals(Integer.class))
            return Integer.class.isAssignableFrom(val.getClass()) ? (Integer) val : Math.toIntExact((Long) val);
        return clazz.cast(val);
    }

    /**
     * Decrement an Id counter.
     * <p>
     * This is primarily used to reserve a range of Ids for use and management of reserved Ids must be handled explicitly.
     *
     * @param name   name of Counter to operate on
     * @param amount amount on Counter to decrement
     * @return value of counter after operation
     */
    public long decrementIdCounter(final String name, final long amount) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER_BIN, -amount);
        final Record record = this.writeOperate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER_BIN));
        return record.getLong(COUNTER_BIN);
    }

    public void setLimitBin(final String name, final long amount) {
        final Key key = new Key(namespace, OLAP_SET, name);
        final Bin ctr = new Bin(OLAP_LIMIT_BIN, amount);
        this.writeOperate(null, key, Operation.put(ctr));
    }

    public Long getLimitBin(final String name) {
        final Key key = new Key(namespace, OLAP_SET, name);
        final Record record = this.writeOperate(null, key, Operation.get(OLAP_LIMIT_BIN));
        return record.getLong(OLAP_LIMIT_BIN);
    }

    public long addLimitBin(final String name, final long amount) {
        final Key key = new Key(namespace, OLAP_SET, name);
        final Bin ctr = new Bin(OLAP_LIMIT_BIN, amount);
        final Record record = this.writeOperate(null, key,
                Operation.add(ctr),
                Operation.get(OLAP_LIMIT_BIN));
        return record.getLong(OLAP_LIMIT_BIN);
    }

    /**
     * Get the time of the last TTL purge was run, and set it to the given time.
     *
     * @param time time to set the last TTL purge ran
     * @return value of when the previous TTL purge was run
     */
    public long getAndSetTtlTime(final long time) {
        final Key key = new Key(namespace, GRAPH_METADATA_SET, TTL_TIME_KEY);
        final Bin timeBin = new Bin(COUNTER_BIN, time);
        final Record record = this.writeOperate(null, key,
                Operation.get(COUNTER_BIN),
                Operation.put(timeBin));
        return record.getLong(COUNTER_BIN);
    }

    /**
     * Drop data in all Aerospike sets associated with currently configured graph by issuing a Truncate operation
     *
     * @param dropIndices drop graph indices
     */
    public void dropDatabase(final FireflyGraph graph, final boolean dropIndices) {
        LOG.info("Dropping database.");
        try {
            // Should never be null in production, but some tests don't have a graph object for a legit reason when
            // calling this function so handle null graph regardless.
            if (graph != null) {
                graph.fireflySummaryUpdater.truncate();
            }

            // If using the client APIs to perform the truncate command on a single-threaded application it is
            // suggested to add a millisecond (ms) sleep. The truncate operation has a 1 millisecond resolution and
            // writes occurring within the same millisecond are not deleted.
            // Source: https://discuss.aerospike.com/t/guidelines-for-deleting-data/3681/1
            Thread.sleep(1);
            client.truncate(null, namespace, EDGE_AERO_SET, null);
            client.truncate(null, namespace, VERTEX_AERO_SET, null);
            client.truncate(null, namespace, USER_SUPPLIED_ID_CACHE_SET, null);
            client.truncate(null, namespace, TEST_SET, null);
            client.truncate(null, namespace, GRAPH_VARIABLES_SET, null);
            client.truncate(null, namespace, GRAPH_METADATA_SET, null);
            client.truncate(null, namespace, INDEX_METADATA_SET, null);
            client.truncate(null, namespace, OUT_VP_SET, null);
            client.truncate(null, namespace, IN_VP_SET, null);
            client.truncate(null, namespace, SUMMARY_SET, null);
            client.truncate(null, namespace, BULK_LOAD_METADATA_SET, null);
            client.truncate(null, namespace, BULK_LOAD_RECOVERY_VERTEX_SET, null);
            client.truncate(null, namespace, BULK_LOAD_RECOVERY_EDGE_SET, null);
            client.truncate(null, namespace, BULK_LOAD_RECOVERY_SUPERNODE_SET, null);
            client.truncate(null, namespace, BULK_LOAD_RECOVERY_STATE_SET, null);

            // Note - we do not delete the id manager set here. This is because Firefly instances hold a reference to the
            // id manager set and if we delete it here, they will likely insert a record with the same id as the one
            // we will eventually reach as we wrap around.
            if (dropIndices)
                dropGraphIndices(graph);
            Thread.sleep(1);
        } catch (final InterruptedException e) {
            // Why would anyone invoke this method in a runner thread that can also have interrupt() called on it? Who
            // knows - just be amazed that they did it with 1ms precision and handle it anyway.
            LOG.warn("InterruptedException caught during database truncate: ", e);
            Thread.currentThread().interrupt();
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    /**
     * Delete all data from the namespace
     */
    public void clearNamespace() {
        client.truncate(null, namespace, null, null);

        final List<Map.Entry<String, String>> indexes = InfoOps.listExistingIndexes(this);
        for (final Map.Entry<String, String> entry : indexes) {
            dropIndex(entry.getValue(), entry.getKey());
        }
    }

    /**
     * Drop an Aerospike Index.
     *
     * @param set       Set name
     * @param indexName Index name
     */
    public void dropIndex(final String set, final String indexName) {
        LOG.debug("Dropping index {}:{}.", set, indexName);
        final Policy policy = new Policy();
        configureWritePolicy(policy);
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            final IndexTask task = client.dropIndex(policy, namespace, set, indexName);
            task.waitTillComplete(1);
        } catch (final AerospikeException e) {
            if (e.getResultCode() == ResultCode.ROLE_VIOLATION) {
                final AerospikeGraphException noAuthException = new AerospikeGraphException(GraphError.DROP_INDEX_UNAUTHORIZED);
                LOG.error(noAuthException.getMessage());
                throw noAuthException;
            } else {
                throw fromAerospikeException(e);
            }
        }
    }

    /**
     * Drop an Aerospike Index.
     *
     * @param set       Set name
     * @param indexName Index name
     */
    public void dropIndexBackground(final String set, final String indexName) {
        LOG.debug("Dropping index {}:{}.", set, indexName);
        final Policy policy = new Policy();
        configureWritePolicy(policy);
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            client.dropIndex(policy, namespace, set, indexName);
        } catch (final AerospikeException e) {
            if (e.getResultCode() == ResultCode.ROLE_VIOLATION) {
                final AerospikeGraphException noAuthException = new AerospikeGraphException(GraphError.DROP_INDEX_UNAUTHORIZED);
                LOG.error(noAuthException.getMessage());
                throw noAuthException;
            } else {
                throw fromAerospikeException(e);
            }
        }
    }

    /**
     * Create an Aerospike Index for a specific key and value type in the key-value pair map of properties.
     *
     * @param existingIndexes
     * @param set                 Set name
     * @param indexName           Index name
     * @param binName             Bin name to be indexed
     * @param type                Index type
     * @param indexCollectionType Index Collection Type
     */
    public void createIndex(
            final List<String> existingIndexes,
            final String set,
            final String indexName,
            final String binName,
            final IndexType type,
            final IndexCollectionType indexCollectionType
    ) {
        if (set.contains(WarmupUtil.getWarmupArenaName()))
            return;
        if (existingIndexes.contains(indexName)) {
            LOG.debug("Index {} already exists", indexName);
            return;
        } else {
            LOG.info("Creating index {}:{}:{}.", set, indexName, binName);
        }
        final Policy policy = new Policy();
        configureWritePolicy(policy);
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            final IndexTask task = client.createIndex(policy, namespace, set, indexName, binName, type, indexCollectionType);
            task.waitTillComplete(1);
            LOG.debug("Completed create index {}", indexName);
        } catch (final AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw fromAerospikeException(ae);
            }
        }
    }

    public void createIndexBackground(
            final List<String> existingIndexes,
            final String set,
            final String indexName,
            final String binName,
            final IndexType type,
            final IndexCollectionType indexCollectionType,
            final boolean errorOnDuplicate,
            final CTX... ctx
    ) {
        if (set.contains(WarmupUtil.getWarmupArenaName()))
            return;

        if (existingIndexes.contains(indexName)) {
            if (errorOnDuplicate) {
                throw new RuntimeException("Index " + indexName + " already exists");
            } else {
                LOG.debug("Index {} already exists", indexName);
                return;
            }
        }
        LOG.info("Creating index {}:{}:{}.", set, indexName, binName);

        final Policy policy = new Policy();
        configureWritePolicy(policy);
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            client.createIndex(policy, namespace, set, indexName, binName, type, indexCollectionType, ctx);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    public String getVpIndexPrefix() {
        return String.format("%s_%s", GRAPH_ID, VP_INDEX_PREFIX);
    }

    public String getEpIndexPrefix() {
        return String.format("%s_%s", GRAPH_ID, EP_INDEX_PREFIX);
    }

    /**
     * Wrapper for AerospikeConnection.operate() to handle returning Firefly exceptions.
     *
     * @param writePolicy WritePolicy for operate.
     * @param key         Key for operate.
     * @param operations  Operations for operate.
     * @return Record resulting from operate.
     */
    private Record operate(final WritePolicy writePolicy, final Key key, final Operation... operations) {
        if (writePolicy == null) {
            // This should never happen.
            throw new IllegalArgumentException("Operate policy must be set.");
        }
        try {
            return this.client.operate(writePolicy, key, operations);
        } catch (final AerospikeException ae) {
            final AerospikeGraphException age = fromAerospikeException(ae);
            final boolean bulkLoading = conf.getBoolean(ConfigurationHelper.Keys.BULK_LOADER_FLAG, false);
            if (!bulkLoading) {
                LOG.error(age.getMessage());
            }
            throw age;
        }
    }

    public Record writeKeyLock(final Key key, final int ttlMillis) {
        final WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        policy.expiration = ttlMillis / 1000;
        configureWritePolicy(policy);
        final Operation createLockRecord = Operation.put(new Bin(this.USER_KEY_BIN, false));
        try {
            return this.client.operate(policy, key, createLockRecord);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    public Record writeOperate(final WritePolicy writePolicy, final Key key, final Operation... operations) {
        final WritePolicy policy;
        if (writePolicy == null) {
            policy = new WritePolicy();
        } else {
            policy = writePolicy;
        }
        configureWritePolicy(policy);

        final FireflyCache cache = transactionCache.get();
        final FireflyCache noPropsCache = emptyPropsTransactionCache.get();
        if (cache != null) {
            cache.invalidate(key);
        }
        if (noPropsCache != null) {
            noPropsCache.invalidate(key);
        }

        return operate(policy, key, operations);
    }

    public Record readOperate(final WritePolicy writePolicy, final Key key, final Operation... operations) {
        final WritePolicy policy;
        if (writePolicy == null) {
            policy = new WritePolicy();
        } else {
            policy = writePolicy;
        }
        configureReadPolicy(policy);

        return operate(policy, key, operations);
    }

    public void truncate(final InfoPolicy policy, final String set, final Calendar beforeLastUpdate) {
        try {
            this.client.truncate(policy, this.namespace, set, beforeLastUpdate);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    public void scanAll(final RecordSequenceListener listener, final ScanPolicy policy, final String set,
                        final String... binNames) {
        final ScanPolicy scanPolicy = policy == null ? new ScanPolicy() : policy;
        configureScanPolicy(scanPolicy);
        try {
            this.client.scanAll(this.eventLoops.next(), listener, scanPolicy, this.namespace, set, binNames);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    public void scanAll(final ScanPolicy policy, final String setName, final ScanCallback callback,
                        final String... binNames) {
        final ScanPolicy scanPolicy = policy == null ? new ScanPolicy() : policy;
        configureScanPolicy(scanPolicy);
        try {
            this.client.scanAll(scanPolicy, this.namespace, setName, callback, binNames);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    public void scanPartitions(final RecordSequenceListener listener, final ScanPolicy policy,
                               final PartitionFilter filter, final String set, final String... binNames) {
        final ScanPolicy scanPolicy = policy == null ? new ScanPolicy() : policy;
        configureScanPolicy(scanPolicy);
        try {
            this.client.scanPartitions(this.eventLoops.next(), listener, scanPolicy, filter, this.namespace, set, binNames);
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    public FireflyRecordSet queryPartitions(final QueryPolicy policy, final Statement statement, final PartitionFilter filter) {
        final QueryPolicy queryPolicy = policy == null ? new QueryPolicy() : policy;
        configureReadPolicy(queryPolicy);
        try {
            return new FireflyRecordSet(this.client.queryPartitions(queryPolicy, statement, filter));
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    public void configureWritePolicy(final Policy policy) {
        policy.maxRetries = AEROSPIKE_MAX_RETRIES;
        policy.sleepBetweenRetries = WRITE_SLEEP_BETWEEN_RETRY;
        policy.totalTimeout = WRITE_TOTAL_TIMEOUT;
        policy.socketTimeout = WRITE_SOCKET_TIMEOUT;
        policy.connectTimeout = CONNECT_TIMEOUT;
        policy.timeoutDelay = TIMEOUT_DELAY;
    }

    public void configureReadPolicy(final Policy policy) {
        final boolean bulkLoading = conf.getBoolean(ConfigurationHelper.Keys.BULK_LOADER_FLAG, false);
        policy.maxRetries = AEROSPIKE_MAX_RETRIES;
        policy.connectTimeout = CONNECT_TIMEOUT;
        policy.timeoutDelay = TIMEOUT_DELAY;
        policy.sleepBetweenRetries = READ_SLEEP_BETWEEN_RETRY;
        if (bulkLoading) {
            policy.totalTimeout = READ_TOTAL_TIMEOUT_BULK_LOAD;
            policy.socketTimeout = READ_SOCKET_TIMEOUT_BULK_LOAD;
        } else if (olapEnabledFlag) {
            policy.totalTimeout = READ_TOTAL_TIMEOUT_BULK_LOAD;
            policy.socketTimeout = READ_SOCKET_TIMEOUT_BULK_LOAD;
        } else {
            policy.totalTimeout = READ_TOTAL_TIMEOUT;
            policy.socketTimeout = READ_SOCKET_TIMEOUT;
        }
    }

    public void configureScanPolicy(final Policy policy) {
        policy.maxRetries = AEROSPIKE_MAX_RETRIES;
        policy.sleepBetweenRetries = READ_SLEEP_BETWEEN_RETRY;
        policy.totalTimeout = SCAN_TOTAL_TIMEOUT;
        policy.socketTimeout = SCAN_SOCKET_TIMEOUT;
        policy.connectTimeout = SCAN_CONNECT_TIMEOUT;
        policy.timeoutDelay = SCAN_TIMEOUT_DELAY;
    }

    public void configureIndexPolicy(final Policy policy) {
        policy.maxRetries = AEROSPIKE_MAX_RETRIES;
        policy.sleepBetweenRetries = READ_SLEEP_BETWEEN_RETRY;
        policy.totalTimeout = INDEX_TOTAL_TIMEOUT;
        policy.socketTimeout = INDEX_SOCKET_TIMEOUT;
        policy.connectTimeout = INDEX_CONNECT_TIMEOUT;
        policy.timeoutDelay = INDEX_TIMEOUT_DELAY;
    }

    /**
     * Initialize the metadata set for bulk loading
     */
    public void initializeBulkLoadMetadata() {
        final Key duplicateVertexIdCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_DUPLICATE_VERTEX_COUNT_KEY));
        final Key badEdgeCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_BAD_EDGES_COUNT_KEY));
        final Key badEntryCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_BAD_ENTRY_COUNT_KEY));

        final Bin zeroValue = new Bin(COUNTER_BIN, 0L);
        final Operation zeroCounter = Operation.put(zeroValue);
        this.writeOperate(null, duplicateVertexIdCountKey, zeroCounter);
        this.writeOperate(null, badEdgeCountKey, zeroCounter);
        this.writeOperate(null, badEntryCountKey, zeroCounter);

        try {
            client.truncate(null, namespace, BULK_LOAD_DUPLICATE_VID_SET, null);
            client.truncate(null, namespace, BULK_LOAD_BAD_EDGE_SET, null);
            client.truncate(null, namespace, BULK_LOAD_BAD_ENTRY_SET, null);
            Thread.sleep(1);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final AerospikeException e) {
            throw fromAerospikeException(e);
        }
    }

    public boolean getBulkLoaderFlag() {
        return this.bulkLoaderFlag;
    }

    public boolean getOlapFlag() {
        return this.olapEnabledFlag;
    }

    public long incrementAndGetBadEdgeCount(final long amount) {
        final Key badEdgeCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_BAD_EDGES_COUNT_KEY));
        final Bin addBin = new Bin(COUNTER_BIN, amount);
        final Record record = this.writeOperate(null, badEdgeCountKey, Operation.add(addBin), Operation.get(COUNTER_BIN));
        return record.getLong(COUNTER_BIN);
    }

    public long incrementAndGetDuplicateVertexIdCount(final long amount) {
        final Key badVertexIdCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_DUPLICATE_VERTEX_COUNT_KEY));
        final Bin addBin = new Bin(COUNTER_BIN, amount);
        final Record record = this.writeOperate(null, badVertexIdCountKey, Operation.add(addBin), Operation.get(COUNTER_BIN));
        return record.getLong(COUNTER_BIN);
    }

    public long incrementAndGetBadEntryCount(final long amount) {
        final Key badEntryCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_BAD_ENTRY_COUNT_KEY));
        final Bin addBin = new Bin(COUNTER_BIN, amount);
        final Record record = this.writeOperate(null, badEntryCountKey, Operation.add(addBin), Operation.get(COUNTER_BIN));
        return record.getLong(COUNTER_BIN);
    }

    public void commit(final Txn txn) {
        try {
            if (txn != null) {
                this.client.commit(txn);
            }
        } catch (final AerospikeException e) {
            LOG.error("Error - AerospikeException in transaction commit: {}", e.getMessage());
            throw fromAerospikeException(e);
        }
    }

    public void rollback(final Txn txn) {
        try {
            if (txn != null) {
                this.client.abort(txn);
            }
        } catch (final AerospikeException e) {
            LOG.error("Error - AerospikeException in transaction abort: {}", e.getMessage());
            throw fromAerospikeException(e);
        }
    }

    public void validateMrtSupport() {
        FireflyAerospikeVersionCheck.validateVersion(client, true);

        try {
            final FireflyId id = this.getIdFactory().getTestId("test");
            final Bin bin = new Bin("txn", "support check");
            final Key key = getKey(this, this.TEST_SET, id);

            final Txn txn = new Txn();
            final WritePolicy writePolicy = new WritePolicy();
            writePolicy.sendKey = true;
            writePolicy.txn = txn;

            checkedPut(writePolicy, key, bin);

            rollback(txn);
        } catch (final AerospikeGraphException e) {
            throw new RuntimeException("Transactions are not supported by Aerospike. Aerospike database must be version 8 or newer with strong consistency mode enabled.");
        }
    }

    public AuthMode getAuthMode() {
        return this.client.getCluster().authMode;
    }

    public boolean getClusterIsActive() {
        return this.client.getCluster().isActive();
    }

    public String getClusterName() {
        return this.client.getCluster().getClusterName();
    }

    public boolean getClusterIsConnected() {
        return this.client.getCluster().isConnected();
    }

    @Override
    public final String toString() {
        return String.format("Aerospike Graph on namespace %s", namespace);
    }

    /**
     * close the connection to Aerospike
     */
    @Override
    public void close() {
        try {
            DefaultAerospikeClientProvider.INSTANCE.close();
        } catch (final Exception e) {
            LOG.error("Error closing Aerospike client", e);
        }
    }

    public static class FireflyRecordSet implements Iterable<KeyRecord>, Closeable {
        private final RecordSet recordSet;

        private FireflyRecordSet(final RecordSet recordSet) {
            this.recordSet = recordSet;
        }

        public boolean next() {
            try {
                return this.recordSet.next();
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        public void close() {
            try {
                this.recordSet.close();
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        public Iterator<KeyRecord> iterator() {
            try {
                return new FireflyKeyRecordIterator(this.recordSet.iterator());
            } catch (final AerospikeException e) {
                throw fromAerospikeException(e);
            }
        }

        private static class FireflyKeyRecordIterator implements Iterator<KeyRecord> {
            private final Iterator<KeyRecord> recordSetIterator;

            private FireflyKeyRecordIterator(final Iterator<KeyRecord> recordSetIterator) {
                this.recordSetIterator = recordSetIterator;
            }

            @Override
            public boolean hasNext() {
                try {
                    return this.recordSetIterator.hasNext();
                } catch (final AerospikeException e) {
                    throw fromAerospikeException(e);
                }
            }

            @Override
            public KeyRecord next() {
                try {
                    return this.recordSetIterator.next();
                } catch (final AerospikeException e) {
                    throw fromAerospikeException(e);
                }
            }
        }
    }

    /**
     * Aerospike client is a singleton per JVM.
     */
    private static class DefaultAerospikeClientProvider implements AerospikeClientProvider, AutoCloseable {
        public static final AtomicLong OPEN_COUNT = new AtomicLong(0);
        public static AerospikeClient CLIENT;
        public static EventLoops EVENT_LOOPS;
        public static ExecutorService THREADED_EXECUTOR_SERVICE;

        public static final DefaultAerospikeClientProvider INSTANCE = new DefaultAerospikeClientProvider();

        private DefaultAerospikeClientProvider() {
        }

        public static AerospikeClientProvider connect(final FireflyConfiguration conf) {
            synchronized (DefaultAerospikeClientProvider.class) {
                if (OPEN_COUNT.get() == 0) {
                    final String eventLoopTypeName = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.EVENT_LOOP_TYPE, conf);
                    final EventLoopType eventLoopType;
                    try {
                        eventLoopType = EventLoopType.valueOf(eventLoopTypeName);
                    } catch (final IllegalArgumentException e) {
                        throw new IllegalArgumentException("Invalid event loop type provided: " + eventLoopTypeName);
                    }
                    final int eventLoopCount = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.EVENT_LOOP_COUNT, conf);
                    final int commandsPerEventLoop = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.COMMANDS_PER_EVENT_LOOP, conf);
                    final int delayQueueSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.DELAY_QUEUE_SIZE, conf);

                    EVENT_LOOPS = initializeEventLoops(eventLoopType, eventLoopCount, commandsPerEventLoop, delayQueueSize);
                    final int threadPoolSize = getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings());
                    final ClientPolicy clientPolicy = setupClientPolicy(conf, threadPoolSize, EVENT_LOOPS);
                    CLIENT = setupDefaultClient(conf, clientPolicy);

                    // Make at least 1 thread available for the threaded executor service, just so it's not empty.
                    final int threadCount = threadPoolSize * ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_BATCH_PER_NODE_THRESHOLD, conf);
                    THREADED_EXECUTOR_SERVICE = Executors.newFixedThreadPool(Math.max(threadCount, 1));
                }
                OPEN_COUNT.incrementAndGet();
                return INSTANCE;
            }
        }

        @Override
        public AerospikeClient getAerospikeClient(final Configuration conf) {
            synchronized (DefaultAerospikeClientProvider.class) {
                if (OPEN_COUNT.get() <= 0 || CLIENT == null || !CLIENT.isConnected()) {
                    throw new RuntimeException("AerospikeClientProvider not connected, call connect(Configuration) first");
                }
                return CLIENT;
            }
        }

        @Override
        public ExecutorService getThreadedExecutorService(final Configuration conf) {
            synchronized (DefaultAerospikeClientProvider.class) {
                if (OPEN_COUNT.get() <= 0 || CLIENT == null || !CLIENT.isConnected()) {
                    throw new RuntimeException("AerospikeClientProvider not connected, call connect(Configuration) first");
                }
                return THREADED_EXECUTOR_SERVICE;
            }
        }

        @Override
        public EventLoops getEventLoops(final Configuration conf) {
            synchronized (DefaultAerospikeClientProvider.class) {
                if (OPEN_COUNT.get() <= 0) {
                    throw new RuntimeException("AerospikeClientProvider not connected, call connect(Configuration) first");
                }
                return EVENT_LOOPS;
            }
        }

        @Override
        public void close() throws Exception {
            synchronized (DefaultAerospikeClientProvider.class) {
                if (OPEN_COUNT.decrementAndGet() == 0) {
                    LOG.info("Closing Aerospike client.");
                    CLIENT.close();
                    EVENT_LOOPS.close();
                    THREADED_EXECUTOR_SERVICE.shutdown();
                    if (!THREADED_EXECUTOR_SERVICE.awaitTermination(1, TimeUnit.SECONDS)) {
                        THREADED_EXECUTOR_SERVICE.shutdownNow();
                    }
                }
                if (OPEN_COUNT.get() < 0) {
                    OPEN_COUNT.set(0);
                }
            }
        }
    }

    /**
     * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
     */

    enum EventLoopType {DIRECT_NIO, NETTY_NIO, NETTY_EPOLL}
}
