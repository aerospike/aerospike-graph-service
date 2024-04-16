package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
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
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.runtime.exceptions.ElementNotFoundException;
import com.aerospike.firefly.runtime.exceptions.RecordTooBigException;
import com.aerospike.firefly.security.JWTAuthenticator;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.DiagnosticUtil;
import com.aerospike.firefly.util.Tokens;
import com.aerospike.firefly.util.WarmupUtil;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.runtime.exceptions.ElementNotFoundException.ELEMENT_NOT_FOUND;
import static com.aerospike.firefly.runtime.exceptions.RecordTooBigException.RECORD_TOO_BIG;
import static com.aerospike.firefly.structure.FireflyGraph.EP_INDEX_PREFIX;
import static com.aerospike.firefly.structure.FireflyGraph.VP_INDEX_PREFIX;
import static com.aerospike.firefly.util.ConfigurationHelper.IMMUTABLE_CONFIG_KEYS;
import static com.aerospike.firefly.util.ConfigurationHelper.getOrDefaultString;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class AerospikeConnection implements AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger(AerospikeConnection.class);
    public final boolean STORAGE_DEBUGGER_FLAG;

    static {
        Value.UseBoolBin = true;
    }

    public static final String DATA_MODEL_KEY = "DATA_MODEL_KEY";
    public static final String DATA_MODEL_NAME = "DATA_MODEL_NAME";
    public static final String DATA_MODEL_VER = "DATA_MODEL_VER";
    public static final String DATA_MODEL_CONF = "DATA_MODEL_CONF";

    public final String GRAPH_ID;
    public final String V_LABEL_INDEX_NAME;
    public final String E_LABEL_INDEX_NAME;
    public final boolean V_LABEL_INDEX_ENABLED_FLAG;
    public final boolean E_LABEL_INDEX_ENABLED_FLAG;
    public final String E_IN_INDEX_NAME;
    public final String E_OUT_INDEX_NAME;
    public static final int NumLoops = 2;
    public static final int CommandsPerEventLoop = 50;
    public static final int DelayQueueSize = 50;
    public final EventLoops eventLoops;
    public final AerospikeClient client;
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
    public final Configuration conf;
    public final String USER_SUPPLIED_ID_CACHE_SET;
    public final List<AbstractMap.Entry<UUID, CompletableFuture<Void>>> cacheTasks;
    public final int AEROSPIKE_WRITE_MAX_RETRY;
    public final long CARDINALITY_METADATA_UPDATE_FREQUENCY;
    public final long INDEX_METADATA_UPDATE_FREQUENCY;
    public final String SUPERNODES_IN_BIN;
    public final String SUPERNODES_OUT_BIN;
    public final String SUPERNODE_EDGE_PROPERTIES_BIN;
    public final String BL_ROW_BIN;
    public final String BL_FILE_BIN;
    public final boolean GLOBAL_EDGE_CACHE_ENABLED_FLAG;
    public final ThreadLocal<FireflyCache> transactionCache = new ThreadLocal<>();
    public final ThreadLocal<ScanHitCounter> scanHitCounterThreadLocal = new ThreadLocal<>();
    public final int AEROSPIKE_BATCH_READ_SIZE;
    public final long FIREFLY_READ_THROUGH_CACHE_WEIGHT;
    public final int PHAT_EDGE_SIZE;
    public final int MOVEMENT_BARRIER_SIZE;
    public final boolean SUMMARY_TICKER_ENABLED_FLAG;
    public final boolean SUMMARY_ENABLED_FLAG;
    public final boolean TTL_ENABLED_FLAG;
    public final boolean TTL_UPDATE_ANYTIME_FLAG;
    public final String TTL_BIN;
    public final String EDGE_DATA_BIN;
    public final String TTL_VERTEX_INDEX_NAME;
    public final String TTL_EDGE_INDEX_NAME;
    public final int TTL_PURGE_INTERVAL_SECONDS;

    public final long PROPERTY_ID_BUFFER_SIZE;
    public final long VERTEX_ID_BUFFER_SIZE;
    public final long EDGE_ID_BUFFER_SIZE;
    public final int CONNECT_TIMEOUT;
    public final int AEROSPIKE_SOCKET_TIMEOUT;
    public final int TIMEOUT_DELAY;
    public final long USAGE_STATS_UPDATE_INTERVAL;
    public final boolean WARMUP_MODE;
    public final boolean PROMETHEUS_RENAME_ENABLED;

    // Bulk Loader fields
    public final Object BL_DUPLICATE_VERTEX_COUNT_KEY;
    public final Object BL_BAD_EDGES_COUNT_KEY;
    public final Object BL_BAD_ENTRY_COUNT_KEY;

    public static final AtomicLong instanceCounter = new AtomicLong(0);

    public final FireflyIdFactory idFactory;
    public final boolean ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY;
    public final boolean ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY;
    public final boolean ENABLE_COMPOSITE_ID_LIMIT_STRATEGY;
    public final boolean ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY;
    public final boolean ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY;
    public final boolean ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY;

    // TODO: Once we are 100% sure these are stable, we can remove the enable flags.
    public final boolean ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY;
    public final boolean ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY;
    public final boolean ENABLE_BATCHED_REPEAT_STEP_STRATEGY;
    public final int PAGINATION_PAGE_QUEUE_SIZE;
    public final int PAGINATION_PAGE_SIZE;
    public final int PAGINATION_PAGE_MAX_WAIT;
    public final boolean AUTHENTICATION_ENABLED;

    public final String QUERY_IMPL;
    public Policy getPolicy() {
        final Policy policy = new Policy();
        policy.connectTimeout = this.CONNECT_TIMEOUT;
        policy.timeoutDelay = this.TIMEOUT_DELAY;
        return policy;
    }

    public static ClientPolicy setupClientPolicy(final Configuration conf, final int threadPoolSize, final EventLoops eventLoops) {
        final ClientPolicy clientPolicy = new ClientPolicy();

        clientPolicy.maxConnsPerNode = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MAX_CONNECTIONS_PER_NODE, conf);
        clientPolicy.minConnsPerNode = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MIN_CONNECTIONS_PER_NODE, conf);

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
        final int port = Integer.parseInt(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_PORT, conf));

        final Optional<String[]> tlsNames;
        if (conf.containsKey(ConfigurationHelper.Keys.TLS_NAMES)) {
            tlsNames = Optional.of(conf.getString(ConfigurationHelper.Keys.TLS_NAMES).split(","));
            if (Host.parseHosts(hostsString, port).length != tlsNames.get().length) {
                throw new IllegalArgumentException("Number of TLS names must match number of hosts");
            }
        } else {
            tlsNames = Optional.empty();
        }
        final Host[] hosts = tlsNames
                .map(tlsNameArray -> Arrays.stream(tlsNameArray)
                        .map(tlsName -> new AbstractMap.SimpleEntry<>(tlsName.split(":")[0], tlsName.split(":")[1]))
                        .map(hostnameTlsNamePair -> new Host(hostnameTlsNamePair.getKey(), hostnameTlsNamePair.getValue(), port))
                        .collect(Collectors.toList()))
                .orElse(Arrays.stream(Host.parseHosts(hostsString, port)).collect(Collectors.toList()))
                .toArray(new Host[0]);

        final AerospikeClient aerospikeClient;
        try {
            aerospikeClient = new AerospikeClient(policy, hosts);
        } catch (final Exception e) {
            LOG.error("Error connecting to Aerospike", e);
            throw e;
        }
        FireflyAerospikeVersionCheck.validateVersion(aerospikeClient);
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
        // Max and min connections per node should be the thread pool size.
        // In batching we may use up to 1 connection per node per thread at a time.
        // Also, we don't want connections recycled, so keep min == max true.
        // We must add 2 because both the metadata updater thread and the cardinality metadata threads using the connection.
        //
        // The bulk loader uses 2 * availableProcessors (+ 2 for the metadata updater thread and the cardinality metadata thread).
        //
        // Because of this, we need to use the greatest of either what the bulk loader would use or what gremlin-server would use.
        return Math.max(2 * Runtime.getRuntime().availableProcessors() + 2, gremlinServerSettings.gremlinPool + 2);
    }


    /**
     * Construct a new AerospikeConnection
     *
     * @param conf Apache Configuration
     */
    public AerospikeConnection(final Configuration conf,
                                final AerospikeClient client,
                                final EventLoops eventLoops) {
        LOG.info("Initializing AerospikeConnection.");
        LOG.debug("CONFIGURATION:");
        conf.getKeys().forEachRemaining(key -> {
            if (!key.contains("password") && !key.contains("secret") && !key.contains("token")) {
                LOG.debug("\tconfig: [{}]:[{}]", key, "*******");
            } else {
                LOG.debug("\tconfig: [{}]:[{}]", key, conf.get(String.class, key));
            }
        });
        LOG.debug("Instance counter: {}", instanceCounter.incrementAndGet());

        this.conf = conf;
        this.namespace = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, conf);
        this.eventLoops = eventLoops;
        this.client = client;

        V_LABEL_INDEX_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG, conf);
        E_LABEL_INDEX_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG, conf);
        GLOBAL_EDGE_CACHE_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED, conf);
        SUMMARY_TICKER_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SUMMARY_TICKER_ENABLED_FLAG, conf);
        SUMMARY_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SUMMARY_ENABLED_FLAG, conf);
        STORAGE_DEBUGGER_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.STORAGE_DEBUGGER_FLAG, conf);
        ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY, conf);
        ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY, conf);
        ENABLE_COMPOSITE_ID_LIMIT_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_LIMIT_STRATEGY, conf);
        ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY, conf);
        ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY, conf);
        ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY, conf);
        ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY, conf);
        ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY, conf);
        ENABLE_BATCHED_REPEAT_STEP_STRATEGY = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCHED_REPEAT_STEP_STRATEGY, conf);
        TTL_ENABLED_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.TTL_ENABLED_FLAG, conf);
        TTL_UPDATE_ANYTIME_FLAG = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.TTL_UPDATE_ANYTIME_FLAG, conf);
        PAGINATION_PAGE_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, conf);
        PAGINATION_PAGE_MAX_WAIT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_MAX_WAIT, conf);
        PAGINATION_PAGE_QUEUE_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_QUEUE_SIZE, conf);
        AUTHENTICATION_ENABLED = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUTHENTICATION_ENABLED, conf);

        GRAPH_VARIABLES_REC_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.GRAPH_VARIABLES_REC_KEY.name(), conf);
        BL_DUPLICATE_VERTEX_COUNT_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.BL_DUPLICATE_VERTEX_COUNT_KEY.name(), conf);
        BL_BAD_EDGES_COUNT_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.BL_BAD_EDGES_COUNT_KEY.name(), conf);
        BL_BAD_ENTRY_COUNT_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.BL_BAD_ENTRY_COUNT_KEY.name(), conf);

        AEROSPIKE_WRITE_MAX_RETRY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_WRITE_MAX_RETRY, conf);

        CARDINALITY_METADATA_UPDATE_FREQUENCY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.CARDINALITY_METADATA_UPDATE_FREQUENCY, conf);
        INDEX_METADATA_UPDATE_FREQUENCY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.INDEX_METADATA_UPDATE_FREQUENCY, conf);
        TTL_PURGE_INTERVAL_SECONDS = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.TTL_PURGE_INTERVAL_SECONDS, conf);

        TEST_SET = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.TEST_SET.name(), conf);
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

        E_IN_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.E_IN_INDEX_NAME.name(), conf));
        E_OUT_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.E_OUT_INDEX_NAME.name(), conf));
        V_LABEL_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.V_LABEL_INDEX_NAME.name(), conf));
        E_LABEL_INDEX_NAME = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.E_LABEL_INDEX_NAME.name(), conf));
        TTL_VERTEX_INDEX_NAME =  String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.TTL_VERTEX_INDEX_NAME.name(), conf));
        TTL_EDGE_INDEX_NAME =  String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.TTL_EDGE_INDEX_NAME.name(), conf));
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

        AEROSPIKE_BATCH_READ_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_BATCH_READ_SIZE, conf);
        FIREFLY_READ_THROUGH_CACHE_WEIGHT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT, conf);
        PHAT_EDGE_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PHAT_EDGE_SIZE, conf);
        MOVEMENT_BARRIER_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MOVEMENT_BARRIER_SIZE, conf);

        PROPERTY_ID_BUFFER_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PROPERTY_ID_BUFFER_SIZE, conf);
        VERTEX_ID_BUFFER_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.VERTEX_ID_BUFFER_SIZE, conf);
        EDGE_ID_BUFFER_SIZE = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE, conf);

        AEROSPIKE_SOCKET_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_SOCKET_TIMEOUT, conf);
        CONNECT_TIMEOUT = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.CONNECT_TIMEOUT, conf);
        TIMEOUT_DELAY = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.TIMEOUT_DELAY, conf);
        USAGE_STATS_UPDATE_INTERVAL = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.USAGE_STATS_UPDATE_INTERVAL, conf);
        WARMUP_MODE = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.WARMUP_MODE, conf);
        PROMETHEUS_RENAME_ENABLED = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.PROMETHEUS_RENAME, conf);

        QUERY_IMPL = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.QUERY_IMPL, conf);

        cacheTasks = new ArrayList<>();
        idFactory = FireflyIdFactory.create(this);

        // Set Edge cache size
        final long onRecordIdMaxLimit = getRecordIdLimitFromAerospike(.9);
        ConfigurationHelper.setOnRecordIdLimit(onRecordIdMaxLimit);
        int onRecordIdLimit;
        try {
            onRecordIdLimit = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, conf);
        } catch (final ConfigurationRuntimeException ignored) {
            // If it was not manually configured, dynamically adjust it relative to the max-record-size configuration of Aerospike
            final long onRecordIdDefaultLimit = getRecordIdLimitFromAerospike(.45);
            onRecordIdLimit = onRecordIdDefaultLimit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) onRecordIdDefaultLimit;
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
    public static AerospikeConnection connect(final Configuration conf,
                                              final AerospikeClient client,
                                              final EventLoops eventLoops) {
        return new AerospikeConnection(conf, client, eventLoops);
    }


    /**
     * Connect to an Aerospike cluster.
     *
     * @param conf Apache Configuration
     * @return Database connection handle
     */
    public static AerospikeConnection connect(final Configuration conf) {
        final AerospikeClientProvider provider = DefaultAerospikeClientProvider.connect(conf);
        return connect(conf, provider.getAerospikeClient(conf), provider.getEventLoops(conf));
    }


    /**
     * Aerospike put with exception handling
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     */
    public void checkedPut(WritePolicy policy, Key key, Bin... bins) {
        try {
            client.put(policy, key, bins);
        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.SERVER_MEM_ERROR)
                LOG.error(Tokens.MEMORY_ERROR_MESSAGE);
            throw e;
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
     * Run a traversal prefetch task
     *
     * @param cacheId
     * @param task    prefetch task to execute
     */
    public void runPrefetchTask(UUID cacheId, Runnable task) {
        if (ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ASYNC_SUBGRAPH_CACHE, this.conf)) {
            cacheTasks.add(new AbstractMap.SimpleEntry<>(cacheId, CompletableFuture.runAsync(task)));
        } else {
            task.run();
        }
    }

    /**
     * Get the idFactory instance
     *
     * @return FireflyIdFactory
     */
    public FireflyIdFactory getIdFactory() {
        return idFactory;
    }

    public EventLoops getEventLoops() {
        return eventLoops;
    }

    public GraphMetadata getDataModelMetadata() {
        final Key k = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        final Policy policy = new Policy();
        policy.sendKey = false;
        Record dataModelRec = read(k, policy);
        return new GraphMetadata(dataModelRec);
    }

    public void setGraphMetadata(final String name, final String version) {
        final Bin dataModelNameBin = new Bin(DATA_MODEL_NAME, name);
        final Operation writeName = Operation.put(dataModelNameBin);
        final Bin dataModelVersionBin = new Bin(DATA_MODEL_VER, version);
        final Operation writeVersion = Operation.put(dataModelVersionBin);

        final Key k = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        this.operate(null, k, writeName, writeVersion);
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
            this.operate(null, key, writeConfig);
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
                this.operate(null, key, newImmutableConfigs.toArray(new Operation[0]));
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
        }

        private static final String MAX_RECORD_SIZE = "max-record-size";
        private static final String STORAGE_ENGINE = "storage-engine";
        private static final String WRITE_BLOCK_SIZE = "storage-engine.write-block-size";
        private static final String STORAGE_ENGINE_PMEM = "pmem";
        private static final String STORAGE_ENGINE_MEMORY = "memory";

        //Parse the whole infoResponse and return it as a List of Maps
        public static List<Map<String, String>> parseRaw(String infoResponse) {
            List<Map<String, String>> results = new ArrayList<>();
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

        public static Map<String, Map<String, String>> parseBySet(String infoResponse, String namespace) {
            Map<String, Map<String, String>> results = new TreeMap<>();
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
         * @param client    client.
         * @param namespace Namespace.
         * @return List of existing indices in a list of map entries.
         * First item of map entry is index
         * Second item of map entry is set the index belongs to
         */
        public static List<Map.Entry<String, String>> listExistingIndexes(final AerospikeClient client, final String namespace) {
            // Using client.getNodes()[0] is okay here since indexes exist across all nodes.
            final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], Keys.SINDEX);
            return parseRaw(infoResponse).stream()
                    .filter(m -> m.get(Keys.NS).equals(namespace))
                    .map(m -> (Map.Entry<String, String>)
                            new AbstractMap.SimpleEntry(m.get(Keys.INDEXNAME), m.get(Keys.SET)))
                    .collect(Collectors.toList());
        }

        /**
         * Return list of usable indices in a list of map entries.
         *
         * @param db    AerospikeConnnection.
         * @param namespace Namespace.
         * @return List of existing indices in a list of map entries.
         * First item of map entry is index
         * Second item of map entry is set the index belongs to
         */
        public static List<String> listUsableIndexes(final AerospikeConnection db, final String namespace) {
            final List<Set<String>> indexSets = new ArrayList<>();
            for (final Node node : db.getClient().getNodes()) {
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
        }

        /**
         * Return the max-record-size configured on Aerospike. If Aerospike is in a cluster, returns the value for the
         * node with the smallest max-record-size.
         *
         * @param client    client.
         * @param namespace Namespace.
         * @return The max-record-size.
         */
        public static long getMaxRecordSizeBytes(final AerospikeClient client, final String namespace) {
            final String requestKey = Keys.GET_CONFIG + namespace;
            final Node[] nodes = client.getNodes();
            long maxRecordSize = Long.MAX_VALUE;

            for (final Node node : nodes) {
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
        }

        /**
         * Is the first connected Aerospike instance "Enterprise Edition"
         *
         * @param client AerospikeClient connection instance
         * @return enterprise or not
         */
        public static boolean isEnterprise(final AerospikeClient client) {
            // Using client.getNodes()[0] is okay here since if one is enterprise, the entire cluster is.
            final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], Keys.FEATURE_KEY);
            return (infoResponse != null && !infoResponse.isEmpty());
        }

        public static String getClusterName(final AerospikeClient client) {
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
            throw new IllegalStateException("Could not find cluster-name in get-config response.");
        }

        /**
         * Get a list of all the Sets in a namespace that have a number of records > 0
         *
         * @param namespace namespace to query
         * @param client    AerospikeClient instance
         * @return Set of namespaces
         */
        public static Set<String> getNonEmptySetList(final String namespace, final AerospikeClient client) {
            final Set<String> allSets = new HashSet<>();

            // Need to loop all nodes here in case one of the sets only has data on a single node.
            for (final Node node : client.getNodes()) {
                final String infoResponse = Info.request(new InfoPolicy(), node, Keys.SETS);
                allSets.addAll(parseBySet(infoResponse, namespace).entrySet().stream().filter(entry -> {
                            Map<String, String> map = entry.getValue();
                            return Integer.parseInt(map.get(Keys.OBJECTS)) > 0;
                        })
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new))
                        .keySet());
            }
            return allSets;
        }
    }


    public static final Map<Class<? extends Serializable>, Class<? extends Serializable>> IdToDiskTypeMap = new HashMap<>() {{
        put(Long.class, Long.class);
        put(Integer.class, Long.class);
        put(Double.class, Double.class);
        put(String.class, Long.class);
    }};
    public static final Map<Class<? extends Serializable>, Long> SupportedValueTypes = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(byte[].class, 4L);
        put(Byte[].class, 4L);
        put(String.class, 5L);
        put(Boolean.class, 6L);
        put(ArrayList.class, 7L);
    }};
    public static final Map<Long, Class<? extends Serializable>> SupportedTypeValues = new HashMap<>() {{
        put(1L, Long.class);
        put(2L, Integer.class);
        put(3L, Double.class);
        put(4L, byte[].class);
        put(5L, String.class);
        put(6L, Boolean.class);
        put(7L, ArrayList.class);
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
        if (!SupportedValueTypes.containsKey(clazz)) {
            throw new UnsupportedOperationException(clazz.getName() + " is not a supported value type");
        } else if (SupportedValueTypes.get(clazz).equals(SupportedValueTypes.get(ArrayList.class))) {
            // Values within a list for our supported types are stored on disk as expected except for Integers which get
            // stored as a Long. We need to keep track of which indexes within the list were inputted as Integers to
            // properly cast them back upon a read.
            final ArrayList<Long> integerIndices = new ArrayList<>();
            final ArrayList<?> valueList = (ArrayList<?>) value;
            for (int i = 0; i < valueList.size(); i++) {
                final Object valueInList = valueList.get(i);
                if (valueInList != null) {
                    final Class valueClass = valueInList.getClass();
                    if (!SupportedValueTypes.containsKey(valueClass) ||
                            SupportedValueTypes.get(valueClass).equals(SupportedValueTypes.get(ArrayList.class))) {
                        throw new UnsupportedOperationException(valueClass.getName()
                                + " within a List is not a supported value type");
                    }
                    if (valueList.get(i).getClass().equals(Integer.class)) {
                        integerIndices.add((long) i);
                    }
                }
            }
            return integerIndices.isEmpty() ? null : integerIndices;
        }
        return Objects.equals(SupportedValueTypes.get(clazz), SupportedValueTypes.get(Integer.class)) ? SupportedValueTypes.get(Integer.class) : null;
    }

    /**
     * Create Indexes for Firefly
     */
    public void createGraphIndexes() {
        final boolean warmup_mode = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.WARMUP_MODE, conf);
        if (warmup_mode || VERTEX_AERO_SET.contains(WarmupUtil.getWarmupArenaName()))
            return;

        LOG.info("Creating graph indices.");
        List<String> existingIndexes =
                InfoOps.listExistingIndexes(getClient(), getNamespace()).stream()
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

        // Create label index in background.
        if (V_LABEL_INDEX_ENABLED_FLAG) {
            createIndexBackground(existingIndexes, setFromElementType(FireflyVertex.class),
                    V_LABEL_INDEX_NAME, LABEL_BIN, IndexType.STRING, IndexCollectionType.DEFAULT, false);
        }
        if (E_LABEL_INDEX_ENABLED_FLAG) {
            // TODO GRAPH-438: Edge indexes.
            throw new RuntimeException("Edge indexes are not currently supported.");
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

    /**
     * Get the AerospikeClient instance used by Firefly
     *
     * @return AerospikeClient instance
     */
    public AerospikeClient getClient() {
        return this.client;
    }

    /**
     * Get the namespace configured for this instance of FireflyGraph
     *
     * @return namespace name
     */
    public String getNamespace() {
        return this.namespace;
    }

    /**
     * Initialize the Aerospike event loops
     *
     * @param eventLoopType
     * @param numLoops
     * @param commandsPerEventLoop
     * @param maxCommandsInQueue
     * @return
     */
    public static EventLoops initializeEventLoops(
            final EventLoopType eventLoopType,
            final int numLoops,
            final int commandsPerEventLoop,
            final int maxCommandsInQueue) {
        final EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.maxCommandsInProcess = commandsPerEventLoop;
        eventPolicy.maxCommandsInQueue = maxCommandsInQueue;
        EventLoops eventLoops = null;
        switch (eventLoopType) {
            case DIRECT_NIO:
                eventLoops = new NioEventLoops(eventPolicy, numLoops);
                break;
            case NETTY_NIO:
                NioEventLoopGroup nioGroup = new NioEventLoopGroup(numLoops);
                eventLoops = new NettyEventLoops(eventPolicy, nioGroup);
                break;
            case NETTY_EPOLL:
                EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(numLoops);
                eventLoops = new NettyEventLoops(eventPolicy, epollGroup);
                break;
            default:
                LOG.error("Error: Invalid event loop type");
        }
        return eventLoops;
    }

    /**
     * perform an Aerospike read by Key
     * pass a Policy (not a BatchPolicy)
     *
     * @param key    Aerospike Key to read
     * @param policy Aerospike Policy to use
     * @return Aerospike Record
     */
    public Record read(final Key key, final Policy policy) {
        readMetric.addAndGet(1);
        final FireflyCache cache = transactionCache.get();
        final Record[] results;
        try { //@todo policy causes key mismatch error
            results = (cache != null) ? new Record[]{cache.read(key)} : new Record[]{client.get(policy, key)};
        } catch (final AerospikeException e) {
            LOG.error("Error: AerospikeException in read {}", e.getMessage());
            throw e;
        }
        return results[0];
    }

    /**
     * Perform a batch Aerospike read for a group of keys
     * defaults to noSendKeyBatchPolicy (subject to change in future version)
     *
     * @param keys Array of Key to return records for
     * @return Array of Record
     */
    public Record[] read(final Key[] keys) {
        final BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.sendKey = false;
        return read(keys, batchPolicy);
    }

    /**
     * Perform a batch Aerospike read for a group of keys
     *
     * @param keys        Array of Key to return records for
     * @param batchPolicy BatchPolicy to use
     * @return Array of Record
     */
    public Record[] read(final Key[] keys, final BatchPolicy batchPolicy) {
        readMetric.addAndGet(keys.length);
        final FireflyCache cache = transactionCache.get();
        final Record[] results;
        try { //@todo policy causes key mismatch error
            results = (cache != null) ? cache.read(keys, batchPolicy) : client.get(batchPolicy, keys);
        } catch (final AerospikeException e) {
            LOG.error("Error: AerospikeException in read {}", e.getMessage());
            throw e;
        }
        return results;
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
        writePolicy.maxRetries = AEROSPIKE_WRITE_MAX_RETRY;
        if (generation != -1) {
            // Set generation for write.
            writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            writePolicy.generation = generation;
        }
        final FireflyCache cache = transactionCache.get();
        if (cache != null) {
            cache.write(writePolicy, key, newBins);
        } else {
            checkedPut(writePolicy, key, newBins);
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
    }

    /**
     * Delete by Key
     *
     * @param key Aerospike Key to delete
     * @return whether record existed on server before deletion
     */
    public boolean delete(final Key key) {
        final FireflyCache cache = transactionCache.get();
        if (cache != null) {
            cache.invalidate(key);
        }
        return client.delete(null, key);
    }

    /**
     * query if the connected Aerospike instance is licenced for Enterprise Edition
     *
     * @return connected Aerospike instance is Enterprise Edition
     */
    public boolean isEnterprise() {
        return InfoOps.isEnterprise(client);
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
                valueList.set(index.intValue(), ((Long) valueList.get(index.intValue())).intValue());
            }
            return valueList;
        }
        final Class clazz = SupportedTypeValues.get(typeHint);
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
        this.operate(null, key, removeValue, removeTypeHint);
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

        this.operate(writePolicy, key, operations);
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
        final Record record = this.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER_BIN));
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
            if (e.getResultCode() == ResultCode.ROLE_VIOLATION) {
                LOG.error("Failed to drop index due to role violation. Please check the permissions of the role assigned.");
            }
            throw e;
        }
    }

    /**
     * Delete all data from the namespace
     */
    public void clearNamespace() {
        final Set<String> sets = InfoOps.getNonEmptySetList(getNamespace(), getClient());
        for (final String set : sets) {
            client.truncate(null, namespace, set, null);
        }
        final List<Map.Entry<String, String>> indexes = InfoOps.listExistingIndexes(getClient(), getNamespace());
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
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            final IndexTask task = client.dropIndex(policy, namespace, set, indexName);
            task.waitTillComplete(1);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.ROLE_VIOLATION) {
                LOG.error("Failed to drop index due to role violation. Please check the permissions of the role assigned.");
            }
            if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
                throw new RuntimeException(ae);
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
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            client.dropIndex(policy, namespace, set, indexName);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.ROLE_VIOLATION) {
                LOG.error("Failed to drop index due to role violation. Please check the permissions of the role assigned.");
            }
            if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
                throw new RuntimeException(ae);
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
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            final IndexTask task = client.createIndex(policy, namespace, set, indexName, binName, type, indexCollectionType);
            task.waitTillComplete(1);
            LOG.debug("Completed create index {}", indexName);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
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
        policy.socketTimeout = 0; // Do not timeout on index create.
        client.createIndex(policy, namespace, set, indexName, binName, type, indexCollectionType, ctx);
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
    public Record operate(final WritePolicy writePolicy, final Key key, Operation... operations) {
        final WritePolicy policy;
        if (writePolicy == null) {
            policy = new WritePolicy();
        } else {
            policy = writePolicy;
        }
        policy.maxRetries = AEROSPIKE_WRITE_MAX_RETRY;

        final FireflyCache cache = transactionCache.get();
        if (cache != null) {
            cache.invalidate(key);
        }

        try {
            return this.getClient().operate(policy, key, operations);
        } catch (final AerospikeException ae) {
            switch (ae.getResultCode()) {
                case ResultCode.RECORD_TOO_BIG:
                    LOG.error("RECORD_TO_BIG error on key {}", key);
                    LOG.error(RECORD_TOO_BIG, ae);
                    throw new RecordTooBigException(ae);
                case ResultCode.KEY_NOT_FOUND_ERROR:
                    LOG.debug(ELEMENT_NOT_FOUND, ae);
                    throw new ElementNotFoundException(ae);
                default:
                    LOG.error(ae.getMessage());
                    throw ae;
            }
        }
    }

    /**
     * Initialize the metadata set for bulk loading
     */
    public void initialzeBulkLoadMetadata() {
        final Key duplicateVertexIdCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_DUPLICATE_VERTEX_COUNT_KEY));
        final Key badEdgeCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_BAD_EDGES_COUNT_KEY));
        final Key badEntryCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_BAD_ENTRY_COUNT_KEY));

        final Bin zeroValue = new Bin(COUNTER_BIN, 0L);
        final Operation zeroCounter = Operation.put(zeroValue);
        this.operate(null, duplicateVertexIdCountKey, zeroCounter);
        this.operate(null, badEdgeCountKey, zeroCounter);
        this.operate(null, badEntryCountKey, zeroCounter);

        try {
            client.truncate(null, namespace, BULK_LOAD_DUPLICATE_VID_SET, null);
            client.truncate(null, namespace, BULK_LOAD_BAD_EDGE_SET, null);
            client.truncate(null, namespace, BULK_LOAD_BAD_ENTRY_SET, null);
            Thread.sleep(1);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long incrementAndGetBadEdgeCount(final long amount) {
        final Key badEdgeCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_BAD_EDGES_COUNT_KEY));
        final Bin addBin = new Bin(COUNTER_BIN, amount);
        final Record record = this.operate(null, badEdgeCountKey, Operation.add(addBin), Operation.get(COUNTER_BIN));
        return record.getLong(COUNTER_BIN);
    }

    public long incrementAndGetDuplicateVertexIdCount(final long amount) {
        final Key badVertexIdCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_DUPLICATE_VERTEX_COUNT_KEY));
        final Bin addBin = new Bin(COUNTER_BIN, amount);
        final Record record = this.operate(null, badVertexIdCountKey, Operation.add(addBin), Operation.get(COUNTER_BIN));
        return record.getLong(COUNTER_BIN);
    }

    public long incrementAndGetBadEntryCount(final long amount) {
        final Key badEntryCountKey = new Key(namespace, BULK_LOAD_METADATA_SET, Value.get(BL_BAD_ENTRY_COUNT_KEY));
        final Bin addBin = new Bin(COUNTER_BIN, amount);
        final Record record = this.operate(null, badEntryCountKey, Operation.add(addBin), Operation.get(COUNTER_BIN));
        return record.getLong(COUNTER_BIN);
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

    /**
     * Aerospike client is a singleton per JVM.
     */
    public static class DefaultAerospikeClientProvider implements AerospikeClientProvider, AutoCloseable {
        public static final AtomicLong OPEN_COUNT = new AtomicLong(0);
        public static AerospikeClient client;
        public static EventLoops eventLoops;

        public static final DefaultAerospikeClientProvider INSTANCE = new DefaultAerospikeClientProvider();

        private DefaultAerospikeClientProvider() {
        }

        public static AerospikeClientProvider connect(final Configuration conf) {
            synchronized (DefaultAerospikeClientProvider.class) {
                if (OPEN_COUNT.get() == 0) {
                    eventLoops = initializeEventLoops(EventLoopType.NETTY_NIO, NumLoops, CommandsPerEventLoop, DelayQueueSize);
                    final int threadPoolSize = getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings());
                    final ClientPolicy clientPolicy = setupClientPolicy(conf, threadPoolSize, eventLoops);
                    client = setupDefaultClient(conf, clientPolicy);
                }
                OPEN_COUNT.incrementAndGet();
                return INSTANCE;
            }
        }

        @Override
        public AerospikeClient getAerospikeClient(final Configuration conf) {
            synchronized (DefaultAerospikeClientProvider.class) {
                if (OPEN_COUNT.get() <= 0 || client == null || !client.isConnected()) {
                    throw new RuntimeException("AerospikeClientProvider not connected, call connect(Configuration) first");
                }
                return client;
            }
        }

        @Override
        public EventLoops getEventLoops(final Configuration conf) {
            synchronized (DefaultAerospikeClientProvider.class) {
                if (OPEN_COUNT.get() <= 0) {
                    throw new RuntimeException("AerospikeClientProvider not connected, call connect(Configuration) first");
                }
                return eventLoops;
            }
        }

        @Override
        public void close() throws Exception {
            synchronized (DefaultAerospikeClientProvider.class) {
                if (OPEN_COUNT.decrementAndGet() == 0) {
                    client.close();
                    eventLoops.close();
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
