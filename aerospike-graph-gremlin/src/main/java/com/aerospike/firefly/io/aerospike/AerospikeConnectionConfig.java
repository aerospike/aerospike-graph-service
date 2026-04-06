package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.BULK_LOADER_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.BULK_LOADER_INITIALIZER_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.MRT_ENABLED_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.TRANSACTION_ENABLED_FLAG;

public class AerospikeConnectionConfig {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeConnectionConfig.class);

    public int version;
    private final MapConfiguration conf;

    public final List<String> vertexMiscBins = new ArrayList<>();
    public final List<String> vertexEdgeBins = new ArrayList<>();
    public final List<String> vertexPropertyBins = new ArrayList<>();

    public static final Set<String> mutableKeys = new HashSet<>() {{
        add("aerospike.client.policy.*");
        add("aerospike.client.batch.read.*");
        add("aerospike.graph.pagination.*");
        add("aerospike.client.scan.max.wait");
        add("aerospike.client.batch-threshold.per-node");
        add("aerospike.graph.movement.barrier.size");
        add("aerospike.client.infopolicy.timeout");
        add("aerospike.graph.cache.*");
    }};

    public final String namespace;
    public final String graphId;
    public final boolean mrtEnabled;
    public final int mrtTimeout;
    public final boolean transactionEnabled;
    public final int transactionTimeout;
    public final String vLabelIndexName;
    public final String eLabelIndexName;
    public final boolean vLabelIndexEnabledFlag;
    public final boolean eLabelIndexEnabledFlag;
    public final String eInIndexName;
    public final String eOutIndexName;

    public final String userKeyBin;
    public final String labelBin;
    public final String inEdgesBin;
    public final String outEdgesBin;
    public final String edgeCacheDisabledBin;
    public final String lockBin;
    public final String indexMetadataSet;

    public final String graphMetadataSet;
    public final String bulkLoadMetadataSet;
    public final String bulkLoadDuplicateVidSet;
    public final String bulkLoadBadEdgeSet;
    public final String bulkLoadBadEntrySet;
    public final String bulkLoadRecoveryVertexSet;
    public final String bulkLoadRecoveryEdgeSet;
    public final String bulkLoadRecoverySupernodeSet;
    public final String bulkLoadRecoveryStateSet;
    public final String bulkLoadRecoveryBin;
    public final String graphVariablesSet;
    public final Object graphVariablesRecKey;
    public final String graphVariablesBin;
    public final String edgeAeroSet;

    public final String vertexAeroSet;
    public final String inVpSet;
    public final String outVpSet;
    public final String summarySet;
    public final String vertexPropertyDataBin;
    public final String vertexPropertyTHBin;
    public final String vpPropertyBin;
    public final String usageStatsSet;
    public final String usageStatsBin;
    public long onRecordIdLimit;
    public final String propertiesBin;
    public final String typeHintsBin;
    public final String counterBin;
    public final String idManagerSet;
    public final String idTypeBin;
    public final String testSet;
    public final String olapJobSet;
    public final String olapTempSet;
    public final String olapAlgorithmTempSet;
    public final String schemaSet;
    public final String schemaBin;

    public final String userSuppliedIdCacheSet;
    public final long cardinalityMetadataUpdateFrequency;
    public final long indexMetadataUpdateFrequency;
    public final boolean configUpdateEnabled;
    public final long configUpdateFrequency;
    public final String supernodesInBin;
    public final String supernodesOutBin;
    public final String blRowBin;
    public final String blFileBin;
    public final boolean globalEdgeCacheEnabledFlag;

    public final long fireflyReadThroughCacheWeight;
    public final String fireflyReadThroughCacheMode;
    public final int phatEdgeSize;
    public final int movementBarrierSize;
    public final boolean summaryTickerEnabledFlag;
    public final int summaryTickerIntervalMs;
    public final boolean summaryEnabledFlag;
    public final boolean ttlEnabledFlag;
    public final String ttlBin;
    public final String edgeDataBin;
    public final String ttlVertexIndexName;
    public final String ttlEdgeIndexName;
    public final int ttlPurgeIntervalSeconds;
    public final boolean supernodesTraversedCounterEnabled;
    public final boolean supernodeTraversalLogWarning;
    public final boolean redactScriptLiteralsEnabled;

    // not used.
    private final int aerospikeTimeout;
    public final int infoTimeout;

    public final long propertyIdBufferSize;
    public final long vertexIdBufferSize;
    public final long edgeIdBufferSize;
    public final long edgeIdRecycleBufferSize;
    public final long usageStatsUpdateInterval;
    public final boolean warmupMode;
    public final boolean prometheusRenameEnabled;

    // Bulk Loader fields
    public final Object blDuplicateVertexCountKey;
    public final Object blBadEdgesCountKey;
    public final Object blBadEntryCountKey;

    public final boolean enableCompositeIdStrategy;
    public final boolean enableEmbeddedCompositeIdStrategy;
    public final boolean enableCompositeIdSamplingStrategy;
    public final boolean enableCompositeIdLimitStrategy;
    public final boolean enableEmbeddedBatchEdgeReadStrategy;
    public final boolean enableBatchVertexReadOtherVStrategy;
    public final boolean enableBatchEdgeToVertexReadStrategy;
    public final boolean enableBatchEdgeReadSamplingStrategy;
    public final boolean enableBatchEdgeReadLimitStrategy;
    public final boolean enableCachedAdjacentIdStrategy;
    public final boolean enableFastHasIdVertexFilterStrategy;

    // MergeEdge fields
    public final int mergeEdgeEvalTimeout;
    public final int mergeEdgeTtl;
    public final int mergeEdgePollInterval;
    public final boolean mergeEdgeStarvationProtection;
    public boolean expirationEnabled;

    // TODO: Once we are 100% sure these are stable, we can remove the enable flags.
    public final boolean enableEmbeddedGraphCountStrategy;
    public final boolean enableEmbeddedVertexEdgeLocalCountStrategy;
    public final boolean enableBatchedRepeatStepStrategy;

    public final int olapPaginationWorkers;
    public final int olapWorkers;
    public final boolean isAuditLogEnabled;
    public final boolean authenticationEnabled;
    public final boolean usageStatsSetIndexEnabled;

    public final String queryImpl;
    private final boolean scanQueryAllowed;
    private final ThreadLocal<Boolean> allowScanTraversalOption = ThreadLocal.withInitial(() -> null);

    public final boolean rackAware;
    public final int rackId;
    public final List<Integer> rackIds;

    public final boolean compress;
    public final int aerospikeMaxRetries;
    public final int writeSleepBetweenRetry;
    public final int readSleepBetweenRetry;
    public final int writeTotalTimeout;
    public final int readTotalTimeout;
    public final int readTotalTimeoutBulkLoad;
    public final int writeSocketTimeout;
    public final int readSocketTimeout;
    public final int readSocketTimeoutBulkLoad;
    public final int connectTimeout;
    public final int timeoutDelay;

    public final int scanTotalTimeout;
    public final int scanSocketTimeout;
    public final int scanConnectTimeout;
    public final int scanTimeoutDelay;
    public final int scanMaxWait;
    public final int queryTotalTimeout;
    public final int querySocketTimeout;
    public final int queryConnectTimeout;
    public final int queryTimeoutDelay;

    public final int aerospikeBatchReadSize;
    public final int aerospikeBatchThreshold;
    public final int paginationPageQueueSize;
    public final int paginationPageSize;
    public final int paginationPageMaxWait;
    public final int paginationShutdownWait;

    // Flags
    public final boolean bulkLoaderFlag;
    public final boolean bulkLoaderInitializerFlag;
    public final boolean olapEnabledFlag;

    public AerospikeConnectionConfig(final MapConfiguration conf,
                                     final IAerospikeClient client) {
        this(conf, client, 0);
    }

    public AerospikeConnectionConfig(final MapConfiguration conf,
                                     final IAerospikeClient client,
                                     final int version) {
        this.conf = conf;
        this.version = version;

        this.namespace = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, conf);

        aerospikeTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_TIMEOUT, conf);
        infoTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.INFO_TIMEOUT, conf);

        bulkLoaderFlag = ConfigurationHelper.getOrDefaultBool(BULK_LOADER_FLAG, conf);
        bulkLoaderInitializerFlag = ConfigurationHelper.getOrDefaultBool(BULK_LOADER_INITIALIZER_FLAG, conf);
        olapEnabledFlag = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.OLAP_ENABLED, conf);

        vLabelIndexEnabledFlag = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG, conf);
        eLabelIndexEnabledFlag = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG, conf);
        globalEdgeCacheEnabledFlag = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED, conf);
        summaryTickerEnabledFlag = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SUMMARY_TICKER_ENABLED_FLAG, conf);
        summaryTickerIntervalMs = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SUMMARY_TICKER_INTERVAL_MS, conf);
        summaryEnabledFlag = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SUMMARY_ENABLED_FLAG, conf);
        enableEmbeddedCompositeIdStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY, conf);
        enableCompositeIdStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY, conf);
        enableCompositeIdSamplingStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY, conf);
        enableCompositeIdLimitStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_LIMIT_STRATEGY, conf);
        enableEmbeddedBatchEdgeReadStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY, conf);
        enableBatchEdgeReadSamplingStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY, conf);
        enableBatchVertexReadOtherVStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY, conf);
        enableBatchEdgeToVertexReadStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_TO_VERTEX_READ_STRATEGY, conf);
        enableBatchEdgeReadLimitStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY, conf);
        enableFastHasIdVertexFilterStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_FAST_HASID_VERTEX_FILTER_STRATEGY, conf);
        enableEmbeddedGraphCountStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY, conf);
        enableEmbeddedVertexEdgeLocalCountStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY, conf);
        enableBatchedRepeatStepStrategy = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_BATCHED_REPEAT_STEP_STRATEGY, conf);

        final boolean adjacentIdEnabled = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY, conf);

        if (adjacentIdEnabled && !enableCompositeIdStrategy) {
            LOG.warn(new AerospikeGraphException(GraphError.CACHE_ADJACENT_ENABLED_COMPOSITE_ID_DISABLED).getMessage());
            enableCachedAdjacentIdStrategy = false;
        } else {
            enableCachedAdjacentIdStrategy = adjacentIdEnabled;
        }

        ttlEnabledFlag = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.TTL_ENABLED_FLAG, conf);
        olapPaginationWorkers = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.OLAP_PAGINATION_WORKERS, conf);
        olapWorkers = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.OLAP_WORKERS, conf);
        authenticationEnabled = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUTHENTICATION_ENABLED, conf);
        usageStatsSetIndexEnabled = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.USAGE_STATS_SET_INDEX_ENABLED, conf);
        isAuditLogEnabled = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUDIT_LOG_ENABLED, conf);

        if (isAuditLogEnabled && !authenticationEnabled) {
            throw new IllegalStateException("Audit logging requires JWT authentication to be configured.");
        }

        graphVariablesRecKey = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.GRAPH_VARIABLES_REC_KEY.name(), conf);
        blDuplicateVertexCountKey = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.BL_DUPLICATE_VERTEX_COUNT_KEY.name(), conf);
        blBadEdgesCountKey = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.BL_BAD_EDGES_COUNT_KEY.name(), conf);
        blBadEntryCountKey = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.BL_BAD_ENTRY_COUNT_KEY.name(), conf);

        cardinalityMetadataUpdateFrequency = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.CARDINALITY_METADATA_UPDATE_FREQUENCY, conf);
        indexMetadataUpdateFrequency = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.INDEX_METADATA_UPDATE_FREQUENCY, conf);
        configUpdateFrequency = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.CONFIG_UPDATE_FREQUENCY, conf);
        configUpdateEnabled = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.CONFIG_UPDATE_ENABLED, conf);
        ttlPurgeIntervalSeconds = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.TTL_PURGE_INTERVAL_SECONDS, conf);
        supernodeTraversalLogWarning = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SUPERNODE_TRAVERSAL_LOG_WARNING, conf);
        supernodesTraversedCounterEnabled = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SUPERNODES_TRAVERSED_COUNTER_ENABLED, conf);
        redactScriptLiteralsEnabled = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.REDACT_SCRIPT_LITERALS_ENABLED, conf);

        testSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.TEST_SET.name(), conf);
        olapJobSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.OLAP_JOB_SET.name(), conf);
        olapTempSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.OLAP_TEMP_SET.name(), conf);
        olapAlgorithmTempSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.OLAP_ALGORITHM_TEMP_SET.name(), conf);
        summarySet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.SUMMARY_SET.name(), conf);
        graphId = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.GRAPH_ID, conf);
        vertexAeroSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.VERTEX_AERO_SET.name(), conf);
        inVpSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.IN_VP_SET.name(), conf);
        outVpSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.OUT_VP_SET.name(), conf);
        idManagerSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.ID_MANAGER_SET.name(), conf);
        schemaSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.SCHEMA_SET.name(), conf);
        edgeAeroSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.EDGE_AERO_SET.name(), conf);
        graphMetadataSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.GRAPH_METADATA_SET.name(), conf);
        graphVariablesSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.GRAPH_VARIABLES_SET.name(), conf);
        usageStatsSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.USAGE_STATS_SET.name(), conf);
        indexMetadataSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.INDEX_METADATA_SET.name(), conf);
        userSuppliedIdCacheSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.USER_SUPPLIED_ID_CACHE_SET.name(), conf);
        bulkLoadMetadataSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_METADATA_SET.name(), conf);
        bulkLoadDuplicateVidSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_DUPLICATE_VID_SET.name(), conf);
        bulkLoadBadEdgeSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_BAD_EDGE_SET.name(), conf);
        bulkLoadBadEntrySet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_BAD_ENTRY_SET.name(), conf);
        bulkLoadRecoveryVertexSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_RECOVERY_VERTEX_SET.name(), conf);
        bulkLoadRecoveryEdgeSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_RECOVERY_EDGE_SET.name(), conf);
        bulkLoadRecoverySupernodeSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_RECOVERY_SUPERNODE_SET.name(), conf);
        bulkLoadRecoveryStateSet = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Sets.BULK_LOAD_RECOVERY_STATE_SET.name(), conf);

        eInIndexName = String.format("%s_%s", graphId, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.E_IN_INDEX_NAME.name(), conf));
        eOutIndexName = String.format("%s_%s", graphId, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.E_OUT_INDEX_NAME.name(), conf));
        vLabelIndexName = String.format("%s_%s", graphId, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.V_LABEL_INDEX_NAME.name(), conf));
        eLabelIndexName = String.format("%s_%s", graphId, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.E_LABEL_INDEX_NAME.name(), conf));
        ttlVertexIndexName = String.format("%s_%s", graphId, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.TTL_VERTEX_INDEX_NAME.name(), conf));
        ttlEdgeIndexName = String.format("%s_%s", graphId, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.InternalConfigs.TTL_EDGE_INDEX_NAME.name(), conf));
        vertexPropertyDataBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.VERTEX_PROPERTY_DATA_BIN.name(), conf);
        vertexPropertyTHBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.VERTEX_PROPERTY_TH_BIN.name(), conf);
        vpPropertyBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.VP_PROPERTY_BIN.name(), conf);
        propertiesBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.name(), conf);
        typeHintsBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.TYPE_HINTS_BIN.name(), conf);
        counterBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.COUNTER_BIN.name(), conf);
        idTypeBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.ID_TYPE_BIN.name(), conf);
        graphVariablesBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.GRAPH_VARIABLES_BIN.name(), conf);
        inEdgesBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.IN_EDGES_BIN.name(), conf);
        outEdgesBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.OUT_EDGES_BIN.name(), conf);
        edgeCacheDisabledBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.EDGE_CACHE_DISABLED_BIN.name(), conf);
        lockBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.LOCK_BIN.name(), conf);
        schemaBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.SCHEMA_BIN.name(), conf);
        supernodesInBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.SUPERNODES_IN.name(), conf);
        supernodesOutBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.SUPERNODES_OUT.name(), conf);
        labelBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.LABEL_BIN.name(), conf);
        userKeyBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.USER_KEY_BIN.name(), conf);
        ttlBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.TTL_BIN.name(), conf);
        usageStatsBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.USAGE_STATS_BIN.name(), conf);
        edgeDataBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.EDGE_DATA_BIN.name(), conf);
        blRowBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.BL_ROW_BIN.name(), conf);
        blFileBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.BL_FILE_BIN.name(), conf);
        bulkLoadRecoveryBin = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.BL_RECOVERY_BIN.name(), conf);

        fireflyReadThroughCacheWeight = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT, conf);
        fireflyReadThroughCacheMode = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.FIREFLY_READ_THROUGH_CACHE_MODE, conf).toUpperCase();
        phatEdgeSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PHAT_EDGE_SIZE, conf);
        movementBarrierSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MOVEMENT_BARRIER_SIZE, conf);

        propertyIdBufferSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PROPERTY_ID_BUFFER_SIZE, conf);
        vertexIdBufferSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.VERTEX_ID_BUFFER_SIZE, conf);
        edgeIdBufferSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE, conf);
        edgeIdRecycleBufferSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.EDGE_ID_RECYCLE_BUFFER_SIZE, conf);

        mergeEdgeTtl = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MERGE_EDGE_TTL, conf);
        mergeEdgeEvalTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MERGE_EDGE_EVAL_TIMEOUT, conf);
        mergeEdgePollInterval = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MERGE_EDGE_POLL_INTERVAL, conf);
        mergeEdgeStarvationProtection = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.MERGE_EDGE_STARVATION_PROTECTION, conf);

        usageStatsUpdateInterval = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.USAGE_STATS_UPDATE_INTERVAL, conf);
        warmupMode = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.WARMUP_MODE, conf);
        prometheusRenameEnabled = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.PROMETHEUS_RENAME, conf);

        queryImpl = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.QUERY_IMPL, conf);
        scanQueryAllowed = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.SCAN_QUERY_ENABLED, conf);

        compress = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AEROSPIKE_COMPRESS, conf);

        rackAware = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.RACK_AWARE, conf);
        rackId = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.RACK_ID, conf);
        final String rackIdsStr = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.RACK_IDS, conf);
        if (rackIdsStr != null && !rackIdsStr.isBlank()) {
            rackIds = java.util.Arrays.stream(rackIdsStr.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(Integer::parseInt)
                    .collect(java.util.stream.Collectors.toList());
        } else {
            rackIds = new ArrayList<>();
        }

        aerospikeMaxRetries = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_MAX_RETRIES, conf);
        writeSleepBetweenRetry = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.WRITE_SLEEP_BETWEEN_RETRY, conf);
        readSleepBetweenRetry = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_SLEEP_BETWEEN_RETRY, conf);
        writeTotalTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.WRITE_TOTAL_TIMEOUT, conf);
        readTotalTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_TOTAL_TIMEOUT, conf);
        readTotalTimeoutBulkLoad = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_TOTAL_TIMEOUT_BULK_LOAD, conf);
        writeSocketTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.WRITE_SOCKET_TIMEOUT, conf);
        readSocketTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_SOCKET_TIMEOUT, conf);
        readSocketTimeoutBulkLoad = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.READ_SOCKET_TIMEOUT_BULK_LOAD, conf);
        connectTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.CONNECT_TIMEOUT, conf);
        timeoutDelay = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.TIMEOUT_DELAY, conf);

        scanTotalTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SCAN_TOTAL_TIMEOUT, conf);
        scanSocketTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SCAN_SOCKET_TIMEOUT, conf);
        scanConnectTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SCAN_CONNECT_TIMEOUT, conf);
        scanTimeoutDelay = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SCAN_TIMEOUT_DELAY, conf);
        scanMaxWait = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.SCAN_MAX_WAIT, conf);
        queryTotalTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.QUERY_TOTAL_TIMEOUT, conf);
        querySocketTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.QUERY_SOCKET_TIMEOUT, conf);
        queryConnectTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.QUERY_CONNECT_TIMEOUT, conf);
        queryTimeoutDelay = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.QUERY_TIMEOUT_DELAY, conf);

        paginationPageMaxWait = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_MAX_WAIT, conf);
        paginationShutdownWait = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_SHUTDOWN_WAIT, conf);
        paginationPageQueueSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_QUEUE_SIZE, conf);

        aerospikeBatchThreshold = client.getNodes().length *
                ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_BATCH_PER_NODE_THRESHOLD, conf);
        final int batchReadSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_BATCH_READ_SIZE, conf);
        if (batchReadSize == 0) {
            aerospikeBatchReadSize = client.getNodes().length *
                    ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.AEROSPIKE_BATCH_READ_SIZE_PER_NODE, conf);
        } else {
            aerospikeBatchReadSize = batchReadSize;
        }
        final int pageSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, conf);
        if (pageSize == 0) {
            paginationPageSize = client.getNodes().length *
                    ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE_PER_NODE, conf);
        } else {
            paginationPageSize = pageSize;
        }
        LOG.info("Batch read size {}, page size {}", aerospikeBatchReadSize, paginationPageSize);


        mrtTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.MRT_TIMEOUT, conf);
        mrtEnabled = ConfigurationHelper.getOrDefaultBool(MRT_ENABLED_FLAG, conf) && !bulkLoaderFlag;
        transactionTimeout = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.TRANSACTION_TIMEOUT, conf);
        transactionEnabled = ConfigurationHelper.getOrDefaultBool(TRANSACTION_ENABLED_FLAG, conf) && !bulkLoaderFlag;

        vertexMiscBins.add(edgeCacheDisabledBin); // 6
        vertexEdgeBins.add(inEdgesBin); // 7
        vertexEdgeBins.add(outEdgesBin); // 8
        vertexMiscBins.add(idTypeBin); // 12
        vertexMiscBins.add(userKeyBin); // 13
        vertexMiscBins.add(labelBin); // 14

        vertexPropertyBins.add(vertexPropertyDataBin);
        vertexPropertyBins.add(vertexPropertyTHBin);
        vertexPropertyBins.add(vpPropertyBin);
    }

    public MapConfiguration getRawConfig() {
        return conf;
    }

    public boolean isScanQueryAllowed() {
        if (this.olapEnabledFlag || this.bulkLoaderFlag || this.warmupMode) {
            // Special runtime situations that scans should always allow.
            return true;
        }

        final Boolean allowScanOption = this.allowScanTraversalOption.get();
        if (allowScanOption != null) {
            return allowScanOption;
        } else {
            return this.scanQueryAllowed;
        }
    }

    public void setAllowScanTraversalOption(final Boolean allowScan) {
        this.allowScanTraversalOption.set(allowScan);
    }

    public void clearAllowScanTraversalOption() {
        this.allowScanTraversalOption.remove();
    }

    public void validate(final AerospikeConnection connection) {
        // Verify that the namespace is not using a default-ttl.
        if (AerospikeConnection.InfoOps.getIsAerospikeTTLEnabled(connection, namespace)) {
            throw new AerospikeGraphException(GraphError.DEFAULT_TTL_EXISTS);
        }

        // If nsup-period is 0 (disabled), and not TTL without nsup is enabled, we cannot use TTL.
        if (!AerospikeConnection.InfoOps.getIsAerospikeNsupPeriodEnabled(connection, namespace)
                && !AerospikeConnection.InfoOps.getIsTTLWithoutNsupEnabled(connection, namespace)) {
            LOG.warn(GraphError.getMessage(GraphError.NSUP_DISABLED));
            expirationEnabled = false;
        } else {
            expirationEnabled = true;
        }

        // Set Edge cache size
        final long onRecordIdMaxLimit = connection.getRecordIdLimitFromAerospike(.9);
        ConfigurationHelper.setOnRecordIdLimit(onRecordIdMaxLimit);
        int onRecordIdLimit;
        try {
            onRecordIdLimit = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, conf);
            // todo: should be same for TRANSACTION_ENABLED???
            if (mrtEnabled && onRecordIdLimit > 1023) {
                LOG.warn("The provided value for '{}' could not be used and has been instead set to the maximum allowed value of 1023 for when '{}' is set as true.", ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, MRT_ENABLED_FLAG);
            }
        } catch (final ConfigurationRuntimeException e) {
            // If it was not manually configured, dynamically adjust it relative to the max-record-size configuration of Aerospike
            if (conf.containsKey(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT)) {
                throw e;
            }
            final long onRecordIdDefaultLimit = connection.getRecordIdLimitFromAerospike(.45);
            onRecordIdLimit = onRecordIdDefaultLimit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) onRecordIdDefaultLimit;
        }

        // MRT operation can handle only 4096 records, so for Vertex drop we can touch no more than 1023 edges
        // (1023*2*2+1) < 4096
        if (mrtEnabled && onRecordIdLimit > 1023) {
            onRecordIdLimit = 1023;
        }
        LOG.info("{} configured to {}.", ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, onRecordIdLimit);
        this.onRecordIdLimit = onRecordIdLimit;
    }

    private boolean isMutableKey(final String key) {
        final String lowerCaseKey = key.toLowerCase();
        for (final String k : mutableKeys) {
            if (k.equals(lowerCaseKey) || (k.endsWith("*") && lowerCaseKey.startsWith(k.substring(0, k.length() - 1)))) {
                return true;
            }
        }

        return false;
    }

    public AerospikeConnectionConfig update(final MapConfiguration conf, final AerospikeConnection connection, final int version) {
        final Map<String, Object> current = this.conf.getMap();
        final Map<String, Object> updated = conf.getMap();

        for (final String key : updated.keySet()) {
            if (!Objects.equals(current.get(key), updated.get(key)) && !isMutableKey(key)) {
                throw new IllegalArgumentException("Immutable option " + key + " can't be changed.");
            }
            this.conf.setProperty(key, updated.get(key));
        }

        this.version = version;

        if (connection == null) {
            throw new IllegalArgumentException("connection is required");
        }
        final AerospikeConnectionConfig next = new AerospikeConnectionConfig(this.conf, connection.getClient(), version);
        next.validate(connection);
        return next;
    }
}
