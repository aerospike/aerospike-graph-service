package com.aerospike.firefly.util;

import ch.qos.logback.classic.Level;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getDefaultThreadPoolSize;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class ConfigurationHelper {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeConnection.class);
    private static final NumericConfigValidator NUMERIC_CONFIG_VALIDATOR = new NumericConfigValidator();

    private ConfigurationHelper() {
    }

    private static final List PREFIX_MASK = new ArrayList() {{
        add(Keys.GRAPH_ID);
        add(Keys.ON_RECORD_ID_LIMIT);
    }};

    public static class Keys {

        // External Configs
        public static final String AEROSPIKE_HOST = "aerospike.client.host";
        public static final String AEROSPIKE_PORT = "aerospike.client.port";
        public static final String AEROSPIKE_USER = "aerospike.client.user";
        public static final String AEROSPIKE_PASSWORD = "aerospike.client.password";
        public static final String AEROSPIKE_NAMESPACE = "aerospike.client.namespace";
        public static final String SCAN_MAX_WAIT = "aerospike.client.scan.max.wait";
        public static final String AEROSPIKE_BATCH_READ_SIZE = "aerospike.client.batch.read.size";
        public static final String TLS = "aerospike.client.tls";
        public static final String TLS_NAMES = "aerospike.client.tls.name";
        public static final String AUTH_MODE = "aerospike.client.auth.mode";
        public static final String CLIENT_SERVICES_ALTERNATE = "aerospike.client.services.alternate";
        public static final String CLUSTER_NAME = "aerospike.client.cluster.name";

        public static final String LOG_LEVEL = "aerospike.graph.log.level";
        public static final String SUPERNODE_TRAVERSAL_LOG_WARNING = "aerospike.graph.log.supernode.warning";
        public static final String FIREFLY_DATA_MODEL = "aerospike.graph.data.model";
        public static final String V_LABEL_INDEX_ENABLED_FLAG = "aerospike.graph.index.vertex.label.enabled";
        public static final String E_LABEL_INDEX_ENABLED_FLAG = "aerospike.graph.index.edge.label.enabled";
        public static final String SUMMARY_ENABLED_FLAG = "aerospike.graph.summary.enabled";
        public static final String SUMMARY_TICKER_ENABLED_FLAG = "aerospike.graph.summary.ticker.enabled";
        public static final String PHAT_EDGE_SIZE = "aerospike.graph.phat.edge.size";
        public static final String MOVEMENT_BARRIER_SIZE = "aerospike.graph.movement.barrier.size";
        public static final String VERTEX_PROPERTY_INDEXES = "aerospike.graph.index.vertex.properties";
        public static final String EDGE_PROPERTY_INDEXES = "aerospike.graph.index.edge.properties";
        public static final String GRAPH_ID = "aerospike.graph.id";
        public static final String TRAVERSAL_NAME = "aerospike.graph.traversal";
        public static final String HTTP_PORT = "aerospike.graph.http.port";
        public static final String PROMETHEUS_PATH = "aerospike.graph.prometheus.path";
        public static final String HEALTHCHECK_PATH = "aerospike.graph.healthcheck.path";
        public static final String PLUGIN = "aerospike.graph.plugin";
        public static final String TTL_ENABLED_FLAG = "aerospike.graph.ttl.enabled";
        public static final String TTL_PURGE_INTERVAL_SECONDS = "aerospike.graph.ttl.purge.interval";

        public static final String WRITE_SOCKET_TIMEOUT = "aerospike.client.policy.write.socketTimeout";
        public static final String READ_SOCKET_TIMEOUT = "aerospike.client.policy.read.socketTimeout";
        public static final String READ_SOCKET_TIMEOUT_BULK_LOAD = "aerospike.client.bulk-load.policy.read.socketTimeout";
        public static final String WRITE_TOTAL_TIMEOUT = "aerospike.client.policy.write.totalTimeout";
        public static final String READ_TOTAL_TIMEOUT = "aerospike.client.policy.read.totalTimeout";
        public static final String READ_TOTAL_TIMEOUT_BULK_LOAD = "aerospike.client.bulk-load.policy.read.totalTimeout";
        public static final String WRITE_SLEEP_BETWEEN_RETRY = "aerospike.client.policy.write.sleepBetweenRetry";
        public static final String READ_SLEEP_BETWEEN_RETRY = "aerospike.client.policy.read.sleepBetweenRetry";

        // Semi internal semi external configs
        public static final String AEROSPIKE_BATCH_PER_NODE_THRESHOLD = "aerospike.client.batch-threshold.per-node";
        public static final String FIREFLY_READ_THROUGH_CACHE_WEIGHT = "aerospike.graph.cache.weight";
        public static final String INDEX_METADATA_UPDATE_FREQUENCY = "aerospike.graph.admin.metadata.index.update.frequency";
        public static final String CARDINALITY_METADATA_UPDATE_FREQUENCY = "aerospike.graph.admin.metadata.cardinality.update.frequency";
        public static final String ENABLE_FAST_COUNT_STRATEGY = "aerospike.graph.strategy.fast.count.enabled";
        public static final String ENABLE_READ_THROUGH_CACHE = "aerospike.graph.strategy.cache.read.through.enabled";
        public static final String ENABLE_PREFETCH_STRATEGY = "aerospike.graph.strategy.prefetch.enabled";
        public static final String ENABLE_FIREFLY_DROP_STRATEGY = "aerospike.graph.strategy.drop.enabled";
        public static final String ENABLE_COMPOSITE_ID_STRATEGY = "aerospike.graph.strategy.composite.id.enabled";
        public static final String ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY = "aerospike.graph.strategy.composite.id.embedded.enabled";
        public static final String ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY = "aerospike.graph.strategy.composite.id.sampling.enabled";
        public static final String ENABLE_COMPOSITE_ID_LIMIT_STRATEGY = "aerospike.graph.strategy.composite.id.limit.enabled";
        public static final String ENABLE_BATCH_EDGE_READ_STRATEGY = "aerospike.graph.strategy.batch.edge.read.enabled";
        public static final String ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY = "aerospike.graph.strategy.batch.otherV.read.enabled";
        public static final String ENABLE_BATCH_EDGE_TO_VERTEX_READ_STRATEGY = "aerospike.graph.strategy.batch.edge.to.vertex.read.enabled";
        public static final String ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY = "aerospike.graph.strategy.batch.edge.read.embedded.enabled";
        public static final String ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY = "aerospike.graph.strategy.batch.edge.read.sampling.enabled";
        public static final String ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY = "aerospike.graph.strategy.batch.edge.read.limit.enabled";
        public static final String ENABLE_CACHED_ADJACENT_ID_STRATEGY = "aerospike.graph.strategy.cached.adjacent.id.enabled";
        public static final String GLOBAL_EDGE_CACHE_ENABLED = "aerospike.graph.global.edge.cache.enabled";
        public static final String VERTEX_ID_BUFFER_SIZE = "aerospike.graph.vertex.id.buffer.size";
        public static final String EDGE_ID_BUFFER_SIZE = "aerospike.graph.edge.id.buffer.size";
        public static final String PROPERTY_ID_BUFFER_SIZE = "aerospike.graph.property.id.buffer.size";
        public static final String BULK_LOAD_ID_BUFFER_SIZE = "aerospike.graph.bulk.load.id.buffer.size";
        public static final String STORAGE_DEBUGGER_FLAG = "aerospike.graph.storage.debug";

        public static final String MAX_ERROR_RATE = "aerospike.client.clientPolicy.maxErrorRate";
        public static final String MIN_CONNECTIONS_PER_NODE = "aerospike.client.clientPolicy.minConnsPerNode";
        public static final String MAX_CONNECTIONS_PER_NODE = "aerospike.client.clientPolicy.maxConnsPerNode";
        public static final String AEROSPIKE_TIMEOUT = "aerospike.client.clientPolicy.timeout";
        public static final String AEROSPIKE_MAX_RETRIES = "aerospike.client.policy.maxRetries";
        public static final String TIMEOUT_DELAY = "aerospike.client.policy.timeoutDelay";
        public static final String CONNECT_TIMEOUT = "aerospike.client.policy.connectTimeout";

        public static final String MERGE_EDGE_TTL = "aerospike.graph.strategy.merge.edge.lock.timeout";
        public static final String MERGE_EDGE_POLL_INTERVAL = "aerospike.graph.strategy.merge.edge.poll.interval";
        public static final String MERGE_EDGE_EVAL_TIMEOUT = "aerospike.graph.strategy.merge.edge.eval.timeout";
        public static final String MERGE_EDGE_STARVATION_PROTECTION  = "aerospike.graph.strategy.merge.edge.starvation.protection.enabled";

        // TODO: Figure out what scan policy settings can be shared with normal read policy settings and therefore removed
        public static final String SCAN_TOTAL_TIMEOUT = "aerospike.client.policy.scan.totalTimeout";
        public static final String SCAN_SOCKET_TIMEOUT = "aerospike.client.policy.scan.socketTimeout";
        public static final String SCAN_CONNECT_TIMEOUT = "aerospike.client.policy.scan.connectTimeout";
        public static final String SCAN_TIMEOUT_DELAY = "aerospike.client.policy.scan.timeoutDelay";
        public static final String INDEX_TOTAL_TIMEOUT = "aerospike.client.policy.index.totalTimeout";
        public static final String INDEX_SOCKET_TIMEOUT = "aerospike.client.policy.index.socketTimeout";
        public static final String INDEX_CONNECT_TIMEOUT = "aerospike.client.policy.index.connectTimeout";
        public static final String INDEX_TIMEOUT_DELAY = "aerospike.client.policy.index.timeoutDelay";

        // TODO: Once we are 100% sure these are stable, we can remove the enable flags.
        public static final String ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY = "aerospike.graph.strategy.fast.count.embedded.enabled";
        public static final String ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY = "aerospike.graph.strategy.local.fast.count.embedded.enabled";
        public static final String ENABLE_BATCHED_REPEAT_STEP_STRATEGY = "aerospike.graph.strategy.batched.repeat.step.enabled";
        public static final String PAGINATION_PAGE_QUEUE_SIZE = "aerospike.graph.pagination.page.queue.size";
        public static final String PAGINATION_PAGE_SIZE = "aerospike.graph.pagination.page.size";
        public static final String PAGINATION_PAGE_MAX_WAIT = "aerospike.graph.pagination.max.wait";
        public static final String PAGINATION_SHUTDOWN_WAIT = "aerospike.graph.pagination.shutdown.wait";

        // Internal-only configurations
        public static final String AUTO_PRE_HEAT = "aerospike.graph.auto.preheat.enabled";
        public static final String WARMUP_MODE = "aerospike.graph.warmup.mode.enabled";
        public static final String WARMUP_ENABLED = "aerospike.graph.warmup.enabled";
        public static final String FAULT_TEST = "aerospike.graph.warmup.mode.fault.test.enabled";
        public static final String ENABLE_CUSTOM_PROFILE = "aerospike.graph.strategy.profile.custom.enabled";
        public static final String ASCLIENT_LOG_ENABLED = "aerospike.client.logging.enabled";
        public static final String ASYNC_SUBGRAPH_CACHE = "aerospike.graph.async.subgraph.cache.enabled";
        public static final String ON_RECORD_ID_LIMIT = "aerospike.graph.vertex.edge.cache.size";
        public static final String DEBUG_MODE_FLAG = "aerospike.graph.debug.mode.enabled";
        public static final String BULK_LOADER_FLAG = "aerospike.graph.bulk.loading.enabled";
        public static final String USAGE_STATS_UPDATE_INTERVAL = "aerospike.graph.usage.update.interval";

        public static final String CLIENT_FAILURE_TEST = "aerospike.graph.failure.client.enabled";
        public static final String CLIENT_FAILURE_RATE = "aerospike.graph.failure.client.rate";
        public static final String PROMETHEUS_RENAME = "aerospike.graph.prometheus.rename.enabled";
        public static final String VALIDATE_CLUSTER_NAME = "aerospike.client.validate.cluster.name";
        public static final String QUERY_IMPL = "aerospike.graph.query.impl";
        public static final String QUERY_PAGED = "paged";
        public static final String QUERY_LEGACY = "legacy";
        public static final String JWT_SECRET = "aerospike.graph-service.auth.jwt.secret";
        public static final String JWT_ISSUER = "aerospike.graph-service.auth.jwt.issuer";
        public static final String JWT_ALGORITHM = "aerospike.graph-service.auth.jwt.algorithm";
        public static final String AUTHENTICATION_ENABLED = "aerospike.graph-service.auth.enabled";
        public static final String USAGE_STATS_SET_INDEX_ENABLED = "aerospike.graph.usage.index.enabled";
        public static final String AUDIT_LOG_ENABLED = "aerospike.graph.audit.log.enabled";

        public static final String EVENT_LOOP_TYPE = "aerospike.client.eventLoop.type";
        public static final String EVENT_LOOP_COUNT = "aerospike.client.eventLoop.count";
        public static final String COMMANDS_PER_EVENT_LOOP = "aerospike.client.eventLoop.commands";
        public static final String DELAY_QUEUE_SIZE = "aerospike.client.delayQueue.size";

        // to speed up testing
        public static final String HTTP_ENABLED = "aerospike.graph.http.enabled";

        public static class Pair {
            public final int numeric;
            public final String english;

            private Pair(final byte key, final String value) {
                this.numeric = key;
                this.english = value;
            }

            public static Pair of(final byte numeric, final String english) {
                return new Pair(numeric, english);
            }
        }

        public enum Bins {
            GRAPH_VARIABLES_BIN(Pair.of((byte) 1, "GRAPH_VARS")),
            VERTEX_PROPERTY_NAME_TO_VALUE_BIN(Pair.of((byte) 2, "VP_NAME_VAL")),
            VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN(Pair.of((byte) 3, "VP_HINT")),
            RELATIONAL_VERTEX_TYPE_HINT_BIN(Pair.of((byte) 4, "REL_VP_HINT")),
            EDGE_CACHE_DISABLED_BIN(Pair.of((byte) 6, "ECACHE_OFF")),
            IN_EDGES_BIN(Pair.of((byte) 7, "IN_EDGES")),
            OUT_EDGES_BIN(Pair.of((byte) 8, "OUT_EDGES")),
            PROPERTIES_BIN(Pair.of((byte) 9, "PROPERTIES")),
            TYPE_HINTS_BIN(Pair.of((byte) 10, "TYPE_HINTS")),
            COUNTER_BIN(Pair.of((byte) 11, "COUNTER")),
            ID_TYPE_BIN(Pair.of((byte) 12, "ID_TYPE")),
            USER_KEY_BIN(Pair.of((byte) 13, "USER_KEY")),
            LABEL_BIN(Pair.of((byte) 14, "LABEL")),
            SUPERNODES_IN(Pair.of((byte) 15, "SUPERNODE_IN")),
            SUPERNODES_OUT(Pair.of((byte) 16, "SUPERNODE_OUT")),
            VERTEX_PROPERTY_NAME_TO_ID_BIN(Pair.of((byte) 17, "VP_NAME_ID")),
            TTL_BIN(Pair.of((byte) 18, "TTL")),
            USAGE_STATS_BIN(Pair.of((byte) 19, "USAGE_STATS")),
            EDGE_DATA_BIN(Pair.of((byte) 20, "EDGE_DATA")),
            BL_ROW_BIN(Pair.of((byte) 21, "BL_ROW")),
            BL_FILE_BIN(Pair.of((byte) 22, "BL_FILE")),
            SUPERNODE_EDGE_PROPERTIES_BIN(Pair.of((byte) 23, "SUPERNODE_P")),
            BL_RECOVERY_BIN(Pair.of((byte) 24, "RECOVERY_DATA"));

            private final Pair value;

            Bins(final Pair b) {
                this.value = b;
            }

            public Pair getValue() {
                return value;
            }

            public static Set<String> keys() {
                return Arrays.stream(Bins.values()).map(Bins::name).collect(Collectors.toSet());
            }
        }

        public enum InternalConfigs {
            GRAPH_VARIABLES_REC_KEY(Pair.of((byte) 0, "GRAPH_VARS_REC")),
            BL_DUPLICATE_VERTEX_COUNT_KEY(Pair.of((byte) 1, "BL_VID_COUNT")),
            BL_BAD_EDGES_COUNT_KEY(Pair.of((byte) 2, "BL_E_COUNT")),
            BL_BAD_ENTRY_COUNT_KEY(Pair.of((byte) 3, "BL_ENTRY_COUNT")),
            V_LABEL_INDEX_NAME(Pair.of((byte) 4, "V_LABEL_IDX")),
            E_LABEL_INDEX_NAME(Pair.of((byte) 5, "E_LABEL_IDX")),
            E_IN_INDEX_NAME(Pair.of((byte) 6, "E_IN_IDX")),
            E_OUT_INDEX_NAME(Pair.of((byte) 7, "E_OUT_IDX")),
            TTL_EDGE_INDEX_NAME(Pair.of((byte) 8, "TTL_V_IDX")),
            TTL_VERTEX_INDEX_NAME(Pair.of((byte) 9, "TTL_E_IDX"));

            private final Pair value;

            InternalConfigs(Pair value) {
                this.value = value;
            }

            public Pair getValue() {
                return value;
            }

            public static Set<String> keys() {
                return Arrays.stream(InternalConfigs.values()).map(InternalConfigs::name).collect(Collectors.toSet());
            }

        }

        public enum Sets {
            GRAPH_VARIABLES_SET(Pair.of((byte) 0, "GRAPH_VARS")),
            EDGE_AERO_SET(Pair.of((byte) 1, "EDGES")),
            VERTEX_AERO_SET(Pair.of((byte) 2, "VERTICES")),
            IN_VP_SET(Pair.of((byte) 3, "IN_VP")),
            OUT_VP_SET(Pair.of((byte) 4, "OUT_VP")),
            ID_MANAGER_SET(Pair.of((byte) 9, "ID_MANAGER")),
            SUMMARY_SET(Pair.of((byte) 10, "SUMMARY")),
            TEST_SET(Pair.of((byte) 11, "TEST")),
            GRAPH_METADATA_SET(Pair.of((byte) 12, "METADATA")),
            USAGE_STATS_SET(Pair.of((byte) 13, "USAGE_STATS_SET")),
            USER_SUPPLIED_ID_CACHE_SET(Pair.of((byte) 30, "ID_CACHE")),
            INDEX_METADATA_SET(Pair.of((byte) 14, "INDEX_METADATA")),
            BULK_LOAD_METADATA_SET(Pair.of((byte) 15, "BL_METADATA")),
            BULK_LOAD_DUPLICATE_VID_SET(Pair.of((byte) 16, "BL_DUPE_VID")),
            BULK_LOAD_BAD_EDGE_SET(Pair.of((byte) 17, "BL_BAD_EDGE")),
            BULK_LOAD_BAD_ENTRY_SET(Pair.of((byte) 18, "BL_BAD_ENTRY")),
            BULK_LOAD_RECOVERY_VERTEX_SET(Pair.of((byte) 19, "BL_RECOVERY_V")),
            BULK_LOAD_RECOVERY_EDGE_SET(Pair.of((byte) 20, "BL_RECOVERY_E")),
            BULK_LOAD_RECOVERY_SUPERNODE_SET(Pair.of((byte) 21, "BL_RECOVERY_SN")),
            BULK_LOAD_RECOVERY_STATE_SET(Pair.of((byte) 22, "BL_RECOVERY_S"));

            private final Pair value;

            public static Set<String> keys() {
                return Arrays.stream(Sets.values()).map(Sets::name).collect(Collectors.toSet());
            }

            public Pair getValue() {
                return value;
            }

            Sets(final Pair names) {
                this.value = names;
            }
        }
    }

    public static final Set<String> IMMUTABLE_CONFIG_KEYS = Set.of(
            Keys.PHAT_EDGE_SIZE, // Calculating the PK wouldn't work
            Keys.SUMMARY_ENABLED_FLAG, // Inaccurate and therefore useless if toggled
            Keys.FIREFLY_DATA_MODEL,
            Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY
    );

    private static final Map<Object, String> defaultValues = new HashMap<>() {{
        put(Keys.AEROSPIKE_HOST, "localhost");
        put(Keys.AEROSPIKE_NAMESPACE, "test");
        put(Keys.AEROSPIKE_USER, "");
        put(Keys.AEROSPIKE_PASSWORD, "");
        put(Keys.GRAPH_ID, "0");
        put(Keys.STORAGE_DEBUGGER_FLAG, "false");
        put(Keys.FIREFLY_DATA_MODEL, "packed");
        put(Keys.V_LABEL_INDEX_ENABLED_FLAG, "false");
        put(Keys.E_LABEL_INDEX_ENABLED_FLAG, "false");
        put(Keys.SCAN_MAX_WAIT, "2000");
        put(Keys.AEROSPIKE_MAX_RETRIES, "2");
        put(Keys.PAGINATION_PAGE_QUEUE_SIZE, "10");
        put(Keys.PAGINATION_PAGE_SIZE, "2048");
        put(Keys.PAGINATION_PAGE_MAX_WAIT, "1200000"); // 20 minutes.
        put(Keys.PAGINATION_SHUTDOWN_WAIT, "0");
        put(Keys.ENABLE_FAST_COUNT_STRATEGY, "true");
        put(Keys.ENABLE_READ_THROUGH_CACHE, "true");
        put(Keys.ENABLE_PREFETCH_STRATEGY, "true");
        put(Keys.ENABLE_FIREFLY_DROP_STRATEGY, "true");
        put(Keys.ENABLE_COMPOSITE_ID_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY, "true");
        put(Keys.ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY, "true");
        put(Keys.ENABLE_COMPOSITE_ID_LIMIT_STRATEGY, "true");
        put(Keys.ENABLE_BATCH_EDGE_READ_STRATEGY, "true");
        put(Keys.ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY, "true");
        put(Keys.ENABLE_BATCH_EDGE_TO_VERTEX_READ_STRATEGY, "true");
        put(Keys.ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY, "true");
        put(Keys.ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY, "true");
        put(Keys.ENABLE_BATCHED_REPEAT_STEP_STRATEGY, "true");
        put(Keys.ASYNC_SUBGRAPH_CACHE, "false");
        put(Keys.AEROSPIKE_PORT, "3000");
        put(Keys.AEROSPIKE_TIMEOUT, "2000");
        put(Keys.WRITE_SOCKET_TIMEOUT, "500");
        put(Keys.READ_SOCKET_TIMEOUT, "50");
        put(Keys.READ_SOCKET_TIMEOUT_BULK_LOAD, "2000");
        put(Keys.VERTEX_ID_BUFFER_SIZE, "1000");
        put(Keys.EDGE_ID_BUFFER_SIZE, "10000");
        put(Keys.PROPERTY_ID_BUFFER_SIZE, "10000");
        put(Keys.BULK_LOAD_ID_BUFFER_SIZE, "2000000");
        put(Keys.CARDINALITY_METADATA_UPDATE_FREQUENCY, "3600000"); // 1 hour default
        put(Keys.INDEX_METADATA_UPDATE_FREQUENCY, "30000"); // 30 second default
        put(Keys.GLOBAL_EDGE_CACHE_ENABLED, "true");
        put(Keys.HTTP_PORT, "9090");
        put(Keys.HTTP_ENABLED, "true");
        put(Keys.PROMETHEUS_PATH, "/metrics");
        put(Keys.HEALTHCHECK_PATH, "/healthcheck");
        put(Keys.AEROSPIKE_BATCH_READ_SIZE, "5000");
        put(Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT, "1000000");
        put(Keys.AEROSPIKE_BATCH_PER_NODE_THRESHOLD, "4");
        put(Keys.VERTEX_PROPERTY_INDEXES, "");
        put(Keys.EDGE_PROPERTY_INDEXES, "");
        put(Keys.PHAT_EDGE_SIZE, "10");
        put(Keys.MOVEMENT_BARRIER_SIZE, "1000");
        put(Keys.LOG_LEVEL, "INFO");
        put(Keys.SUPERNODE_TRAVERSAL_LOG_WARNING, "true");
        put(Keys.TLS, "false");
        put(Keys.AUTO_PRE_HEAT, "true");
        put(Keys.WARMUP_ENABLED, "true");
        put(Keys.WARMUP_MODE, "false");
        put(Keys.ENABLE_CUSTOM_PROFILE, "true");
        put(Keys.FAULT_TEST, "false");
        put(Keys.CLIENT_FAILURE_TEST, "false");
        put(Keys.CLIENT_FAILURE_RATE, "0");
        put(Keys.ASCLIENT_LOG_ENABLED, "false");
        put(Keys.SUMMARY_TICKER_ENABLED_FLAG, "true");
        put(Keys.SUMMARY_ENABLED_FLAG, "true");
        put(Keys.BULK_LOADER_FLAG, "false");
        put(Keys.MAX_ERROR_RATE, "100");
        put(Keys.MAX_CONNECTIONS_PER_NODE, String.valueOf(getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings()) * 2));
        put(Keys.MIN_CONNECTIONS_PER_NODE, String.valueOf(getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings())));
        put(Keys.CONNECT_TIMEOUT, "0");
        put(Keys.TIMEOUT_DELAY, "2000");
        put(Keys.WRITE_TOTAL_TIMEOUT, "2500");
        put(Keys.READ_TOTAL_TIMEOUT, "150");
        put(Keys.READ_TOTAL_TIMEOUT_BULK_LOAD, "6000");
        put(Keys.WRITE_SLEEP_BETWEEN_RETRY, "500");
        put(Keys.READ_SLEEP_BETWEEN_RETRY, "0");
        put(Keys.PROMETHEUS_RENAME, "true");
        put(Keys.DEBUG_MODE_FLAG, "false");
        put(Keys.TTL_ENABLED_FLAG, "false");
        put(Keys.TTL_PURGE_INTERVAL_SECONDS, "2");
        put(Keys.USAGE_STATS_UPDATE_INTERVAL, "3600000"); // 1 hour default
        put(Keys.AUTH_MODE, "internal");
        put(Keys.CLIENT_SERVICES_ALTERNATE, "false");
        put(Keys.CLUSTER_NAME, "");
        put(Keys.VALIDATE_CLUSTER_NAME, "true");
        put(Keys.QUERY_IMPL, Keys.QUERY_PAGED);
        put(Keys.JWT_ALGORITHM, "HMAC256");
        put(Keys.AUTHENTICATION_ENABLED, "false");
        put(Keys.USAGE_STATS_SET_INDEX_ENABLED, "true");
        put(Keys.AUDIT_LOG_ENABLED, "false");
        put(Keys.SCAN_TOTAL_TIMEOUT, "0");
        put(Keys.SCAN_SOCKET_TIMEOUT, "1200000");
        put(Keys.SCAN_CONNECT_TIMEOUT, "0");
        put(Keys.SCAN_TIMEOUT_DELAY, "0");
        put(Keys.INDEX_TOTAL_TIMEOUT, "0");
        put(Keys.INDEX_SOCKET_TIMEOUT, "1200000");
        put(Keys.INDEX_CONNECT_TIMEOUT, "0");
        put(Keys.INDEX_TIMEOUT_DELAY, "0");
        put(Keys.EVENT_LOOP_TYPE, EventLoopType.NETTY_NIO.name());
        put(Keys.EVENT_LOOP_COUNT, "2");
        put(Keys.COMMANDS_PER_EVENT_LOOP, "50");
        put(Keys.DELAY_QUEUE_SIZE, "50");
        put(Keys.MERGE_EDGE_EVAL_TIMEOUT, "10000");
        put(Keys.MERGE_EDGE_TTL, "10000");
        put(Keys.MERGE_EDGE_POLL_INTERVAL, "10");
        put(Keys.MERGE_EDGE_STARVATION_PROTECTION, "false");
        put(Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY, "false");
    }};

    private static final Map<Object, String> BULK_LOAD_DEFAULTS = new HashMap<>() {{
        put(Keys.WRITE_SOCKET_TIMEOUT, "5000");
        put(Keys.WRITE_TOTAL_TIMEOUT, "25000");
        put(Keys.WRITE_SLEEP_BETWEEN_RETRY, "5000");
        put(Keys.MAX_CONNECTIONS_PER_NODE, String.valueOf(getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings())));
    }};

    static {
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.SCAN_MAX_WAIT, 100);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.AEROSPIKE_MAX_RETRIES, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.PAGINATION_PAGE_QUEUE_SIZE, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.PAGINATION_PAGE_SIZE, 128);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.PAGINATION_PAGE_MAX_WAIT, 1000);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.AEROSPIKE_TIMEOUT, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.WRITE_SOCKET_TIMEOUT, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.READ_SOCKET_TIMEOUT, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.READ_TOTAL_TIMEOUT_BULK_LOAD, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.READ_SOCKET_TIMEOUT_BULK_LOAD, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.READ_SOCKET_TIMEOUT, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.WRITE_TOTAL_TIMEOUT, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.READ_TOTAL_TIMEOUT, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.WRITE_SLEEP_BETWEEN_RETRY, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.READ_SLEEP_BETWEEN_RETRY, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.VERTEX_ID_BUFFER_SIZE, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.EDGE_ID_BUFFER_SIZE, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.PROPERTY_ID_BUFFER_SIZE, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.BULK_LOAD_ID_BUFFER_SIZE, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.CARDINALITY_METADATA_UPDATE_FREQUENCY, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.INDEX_METADATA_UPDATE_FREQUENCY, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.TTL_PURGE_INTERVAL_SECONDS, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.AEROSPIKE_BATCH_READ_SIZE, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfig(Keys.PHAT_EDGE_SIZE, 1, 100);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.MOVEMENT_BARRIER_SIZE, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.MAX_ERROR_RATE, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.CONNECT_TIMEOUT, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.TIMEOUT_DELAY, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.USAGE_STATS_UPDATE_INTERVAL, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.MAX_CONNECTIONS_PER_NODE, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.MIN_CONNECTIONS_PER_NODE, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfig(Keys.MERGE_EDGE_EVAL_TIMEOUT, 1, 60000);
        NUMERIC_CONFIG_VALIDATOR.addConfig(Keys.MERGE_EDGE_TTL, 1000, 60000);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.MERGE_EDGE_POLL_INTERVAL, 1);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.EVENT_LOOP_COUNT, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.DELAY_QUEUE_SIZE, 0);
        NUMERIC_CONFIG_VALIDATOR.addConfigMin(Keys.AEROSPIKE_BATCH_PER_NODE_THRESHOLD, 2);
    }

    public static List<String> getOrDefaultList(final String key, final Configuration config) {
        // Adds a space if it is empty. Remove the space.
        final List<String> values = Arrays.stream(getOrDefaultString(key, config).split(",")).map(String::trim).collect(Collectors.toList());
        values.remove("");
        return values;
    }

    public static Configuration loadFromFile(final Path path) {
        try {
            Properties props = new Properties();
            props.load(Files.newBufferedReader(path));
            HashMap<String, Object> configData = new HashMap<>();
            props.keySet().forEach(it -> {
                final String key = it.toString().toLowerCase();
                final Object value = props.get(it.toString());
                if (LOG.isDebugEnabled()) {
                    final Object maskedValue;
                    if (key.contains("password") || key.contains("secret") || key.contains("token") || key.contains("passkey")) {
                        maskedValue = "*******";
                    } else {
                        maskedValue = value;
                    }
                    LOG.debug("config: [{}:{}]", key, maskedValue);
                }
                configData.put(key, value);
            });
            return new MapConfiguration(configData);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Configuration loadFromFile(final String path) {
        return loadFromFile(Paths.get(path));
    }

    public static Configuration loadFromResources(final String name) {
        try (InputStream is = FireflyGraph.class.getClassLoader().getResourceAsStream(name)) {
            if (is == null) throw new RuntimeException("unable to find resource " + name);
            try (final InputStreamReader isr = new InputStreamReader(is);
                 final BufferedReader reader = new BufferedReader(isr)) {
                Properties props = new Properties();
                props.load(reader);
                HashMap<String, Object> configData = new HashMap<>();
                props.keySet().forEach(it -> {
                    configData.put(it.toString().toLowerCase(), props.get(it.toString()));
                });
                return new MapConfiguration(configData);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean checkInternalKeys(final String key) {
        return Keys.InternalConfigs.keys().contains(key) ||
                Keys.Sets.keys().contains(key) ||
                Keys.Bins.keys().contains(key);
    }

    public static Object getOrDefault(final String key, final Configuration config) {
        // Debug mode is a special case.
        if (key.equalsIgnoreCase(Keys.DEBUG_MODE_FLAG)) {
            return (config.containsKey(Keys.DEBUG_MODE_FLAG)) ?
                    config.getString(Keys.DEBUG_MODE_FLAG) : defaultValues.get(Keys.DEBUG_MODE_FLAG);
        }
        if (key.equalsIgnoreCase(Keys.BULK_LOADER_FLAG)) {
            return (config.containsKey(Keys.BULK_LOADER_FLAG)) ?
                    config.getString(Keys.BULK_LOADER_FLAG) : defaultValues.get(Keys.BULK_LOADER_FLAG);
        }

        final String lowerKey = key.toLowerCase();
        final String upperKey = key.toUpperCase();
        final boolean debugMode = getOrDefaultBool(Keys.DEBUG_MODE_FLAG, config);

        if (System.getenv().containsKey(lowerKey) ||
                System.getenv().containsKey(upperKey) ||
                System.getenv().containsKey(key)) {
            String envConfig = System.getenv(upperKey);
            if (envConfig == null || envConfig.isEmpty()) {
                envConfig = System.getenv(lowerKey);
            }
            if (envConfig == null || envConfig.isEmpty()) {
                envConfig = System.getenv(key);
            }
            return envConfig;
        } else if (!config.containsKey(lowerKey) &&
                !config.containsKey(upperKey) &&
                !config.containsKey(key) &&
                !defaultValues.containsKey(key) &&
                !checkInternalKeys(key)) {
            throw new ConfigurationRuntimeException("no default value available for key: " + lowerKey);
        } else if (config.containsKey(lowerKey) || config.containsKey(upperKey) || config.containsKey(key)) {
            String configValue = config.getString(lowerKey, "");
            if (configValue.isEmpty()) {
                configValue = config.getString(upperKey, "");
            }
            if (configValue.isEmpty()) {
                configValue = config.getString(key, "");
            }
            return configValue;
        } else if (Keys.InternalConfigs.keys().contains(key)) {
            if (debugMode) {
                return Keys.InternalConfigs.valueOf(key).getValue().english;
            } else {
                return Keys.InternalConfigs.valueOf(key).getValue().numeric;
            }
        } else if (Keys.Sets.keys().contains(key)) {
            final String prefix = PREFIX_MASK.contains(lowerKey) ? "" : getPrefix(config);
            if (debugMode) {
                return prefix + Keys.Sets.valueOf(key).getValue().english;
            } else {
                return prefix + Keys.Sets.valueOf(key).getValue().numeric;
            }
        } else if (Keys.Bins.keys().contains(key)) {
            if (debugMode) {
                return Keys.Bins.valueOf(key).getValue().english;
            } else {
                return Keys.Bins.valueOf(key).getValue().numeric;
            }
        }
        if (BULK_LOAD_DEFAULTS.containsKey(key) && getOrDefaultBool(Keys.BULK_LOADER_FLAG, config)) {
            return BULK_LOAD_DEFAULTS.get(key);
        }
        return defaultValues.get(key);
    }

    public static String getOrDefaultString(final String key, Configuration config) {
        final Object value = getOrDefault(key, config);
        if (value != null)
            return value.toString();
        else
            return null;
    }

    public static int getOrDefaultInt(final String key, Configuration config) {
        final String value = (String) getOrDefault(key, config);
        return NUMERIC_CONFIG_VALIDATOR.validate(key, value);
    }

    public static boolean getOrDefaultBool(final String key, Configuration config) {
        final String value = (String) getOrDefault(key, config);
        // Use our own parser here to be more strict and robust
        if ("true".equals(value.toLowerCase().trim())) {
            return true;
        } else if ("false".equals(value.toLowerCase().trim())) {
            return false;
        } else {
            final String errorMessage = "Value provided, \"" + value + "\", for configuration key, \"" + key + "\", is not a valid boolean.";
            LOG.error(errorMessage);
            throw new ConfigurationRuntimeException(errorMessage);
        }
    }

    public static String getPrefix(Configuration config) {
        return config.containsKey(Keys.GRAPH_ID.toLowerCase()) ? config.get(String.class, Keys.GRAPH_ID.toLowerCase()) + "_" : defaultValues.get(Keys.GRAPH_ID) + "_";
    }

    public static String aerospikeNamespace(Configuration c) {
        return c.get(String.class, Keys.AEROSPIKE_NAMESPACE.toLowerCase());
    }

    public static int aerospikePort(Configuration c) {
        return c.get(Integer.class, Keys.AEROSPIKE_PORT.toLowerCase());
    }

    public static String aerospikeHost(Configuration c) {
        return c.get(String.class, Keys.AEROSPIKE_HOST.toLowerCase());
    }

    public static String dumpDefaults() {
        final Properties props = new Properties();
        props.putAll(defaultValues);
        final StringWriter sw = new StringWriter();
        try {
            props.store(sw, "FireflyGraph Configuration Defaults");
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return sw.toString();
    }

    public static void validateConfig(final Configuration config) {
        final Field[] keyFields = Keys.class.getFields();
        final Keys keys = new Keys();
        final Set<String> validKeys = Arrays.stream(keyFields).map(f -> {
            try {
                return (String) f.get(keys);
            } catch (final IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toSet());
        validKeys.add("gremlin.graph");

        final Field[] bulkLoaderFields = BulkLoaderConfigHelper.class.getFields();
        final BulkLoaderConfigHelper bulkLoaderConfigHelper = new BulkLoaderConfigHelper(new HashMap<>(), null);
        Arrays.stream(bulkLoaderFields).forEach(f -> {
            try {
                if (f.get(bulkLoaderConfigHelper) instanceof String) {
                    validKeys.add((String) f.get(bulkLoaderConfigHelper));
                }
            } catch (final IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });

        final Iterator<String> configKeys = config.getKeys();
        final List<String> invalidKeys = new ArrayList<>();
        while (configKeys.hasNext()) {
            final String key = configKeys.next();
            if (!validKeys.contains(key)) {
                invalidKeys.add(key);
            }
        }
        if (!invalidKeys.isEmpty()) {
            if (System.getenv("FIREFLY_TESTING") != null && System.getenv("FIREFLY_TESTING").equals("true")) {
                throw new IllegalArgumentException("Error, the following configuration keys are invalid: " + invalidKeys);
            } else {
                LOG.error("ERROR: Aerospike Graph Service was unable to initialize due to invalid configuration keys: {}. Please fix these keys and try again.", invalidKeys);

                // This error comes out in a bunch of massive stack traces and ultimately the container hangs.
                // Disable any more logging and force system to shut down. A bunch of logs come out after this system call which is why it is required.
                LoggerUtil.setLogLevel(Level.OFF);
                System.exit(1);
            }
        }
    }

    public static void setOnRecordIdLimit(final long limit) {
        final int intLimit = limit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) limit;
        NUMERIC_CONFIG_VALIDATOR.addConfig(Keys.ON_RECORD_ID_LIMIT, 0, intLimit);
    }
}
