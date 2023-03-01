package com.aerospike.firefly.io;

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
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertexProperty;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.Tokens;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.configuration2.Configuration;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.utils.ExceptionMessages.ELEMENT_NOT_FOUND;
import static com.aerospike.firefly.io.utils.ExceptionMessages.RECORD_TOO_BIG;
import static com.aerospike.firefly.structure.FireflyGraph.EP_INDEX_PREFIX;
import static com.aerospike.firefly.structure.FireflyGraph.VP_INDEX_PREFIX;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class AerospikeConnection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeConnection.class);
    public static final BatchPolicy noSendKeyBatchPolicy;
    public static final Policy noSendKeyReadPolicy;

    static {
        noSendKeyReadPolicy = new Policy();
        noSendKeyBatchPolicy = new BatchPolicy();
        noSendKeyReadPolicy.sendKey = false;
        noSendKeyBatchPolicy.sendKey = false;
    }

    public static final String USER_KEY = "USER_KEY";
    public static final String LABEL = "label";
    private static final String DATA_MODEL_KEY = "DATA_MODEL_KEY";
    public static final String DATA_MODEL_NAME = "DATA_MODEL_NAME";
    public static final String DATA_MODEL_VER = "DATA_MODEL_VER";

    public final String GRAPH_ID;
    private final String INDEXED_BINS;
    public final String V_LABEL_INDEX;
    public final String E_LABEL_INDEX;
    public final boolean V_LABEL_INDEX_ENABLED;
    public final boolean E_LABEL_INDEX_ENABLED;
    public final String E_IN_INDEX;
    public final String E_OUT_INDEX;
    private static final int NumLoops = 2;
    private static final int CommandsPerEventLoop = 50;
    private static final int DelayQueueSize = 50;

    private final EventLoops eventLoops;

    private final String host;
    private final int port;
    private final AerospikeClient client;
    private final String namespace;

    public final String IN_EDGES;
    public final String OUT_EDGES;
    public final String EDGE_CACHE_DISABLED;
    public final String VP_CACHE_DISABLED;
    public final String RELATIONAL_VERTEX_TYPE_HINT;
    private final String INDEX_METADATA;

    protected final String GRAPH_METADATA_SET;
    public final String GRAPH_VARIABLES_SET;
    public final String GRAPH_VARIABLES_RECORD;
    public final String GRAPH_VARIABLES_MAP;
    public final String EDGE_AERO_SET;

    public final String VERTEX_AERO_SET;
    public final String IN_VP_SET;
    public final String OUT_VP_SET;
    public final String IN_IN_SET;
    public final String IN_OUT_SET;
    public final String OUT_IN_SET;
    public final String OUT_OUT_SET;
    protected final String VERTEX_EDGELIST_AERO_SET;
    public final String VERTEX_PROPERTY_AERO_SET;
    protected final String EDGE_ID_KEY;
    protected final String EDGE_ID_BIN;
    protected final String VERTEX_ID_KEY;
    protected final String VERTEX_ID_BIN;
    protected final String VERTEX_PROPERTY_ID_KEY;
    protected final String VERTEX_PROPERTY_ID_BIN;
    public final String VERTEX_PROPERTY_NAME_TO_ID;
    public final String VERTEX_PROPERTY_NAME_TO_VALUE;
    public final String VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT;
    public final String VERTEX_PROPERTY_NAME;
    public final String EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN;
    public final String PARENT_VERTEX_ID;

    public final String IN_EDGE_COUNTER;
    public final String OUT_EDGE_COUNTER;
    public final String VP_COUNTER;
    public final long ID_CACHE_SIZE;
    public final String PROPERTIES;
    protected final String VP_PROPERTIES;
    public final String TYPE_HINTS;
    public final String VP_TYPE_HINTS;
    public final String KEY_VALUE;
    protected final String COUNTER;
    protected final String ID_MANAGER_SET;
    public final String ID_TYPE_BIN;
    public final String GLOBAL;
    public final String TEST_SET;
    public final Configuration conf;
    public final String USER_SUPPLIED_ID_CACHE_SET;
    public final String USER_SUPPLIED_ID_VERTEX_CACHE;
    public final String USER_SUPPLIED_ID_EDGE_CACHE;
    public final String USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE;
    public final List<AbstractMap.Entry<UUID, CompletableFuture<Void>>> cacheTasks;
    public final int AEROSPIKE_CONNECTION_MAX_RETRY;
    public final long CARDINALITY_METADATA_UPDATE_FREQUENCY;
    public final long INDEX_METADATA_UPDATE_FREQUENCY;
    public final boolean ADJACENCY_INDEX_ENABLED;
    public final boolean EDGE_CACHE_DISABLED_GLOBALLY;
    public final List<String> OPTIMIZED_TWO_HOP_STEPS; // Optionally: ["out_out", "out_in", "in_out", "in_in"].
    public final List<String> OPTIMIZED_HOP_CONSTRAINT_STEPS; // Optionally: ["out_vp", "in_vp"].

    public final ThreadLocal<FireflyCache> transactionCache = new ThreadLocal<>();
    public final int AEROSPIKE_BATCH_READ_SIZE;
    public final long FIREFLY_READ_THROUGH_CACHE_WEIGHT;

    private final List<String> VALID_OPTIMIZED_TWO_HOP_STEPS = Arrays.asList("out_out", "out_in", "in_out", "in_in");
    private final List<String> VALID_OPTIMIZED_HOP_CONSTRAINT_STEPS = Arrays.asList("out_vp", "in_vp");
    private final ScanHitCounter scanHitCounter = ScanHitCounter.create(60, 100, 10, (entry) -> {
        LOG.warn("WARNING: Scan triggered on {} has been hit {} times within 60 seconds, consider adding an index.", entry.getKey(), entry.getValue());
        return null;
    });
    private final FireflyIdFactory idFactory;

    /**
     * Construct a new AerospikeConnection
     *
     * @param conf Apache Configuration
     */
    public AerospikeConnection(final Configuration conf) {
        LOG.info("Initializing AerospikeConnection.");
        LOG.debug("CONFIGURATION:");
        conf.getKeys().forEachRemaining(key -> LOG.debug("\tconfig: [{}]:[{}]", key, conf.get(String.class, key)));
        this.conf = conf;
        this.host = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AEROSPIKE_HOST, conf);
        this.port = Integer.parseInt(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AEROSPIKE_PORT, conf));
        this.namespace = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, conf);

        this.eventLoops = initializeEventLoops(EventLoopType.DIRECT_NIO, NumLoops, CommandsPerEventLoop, DelayQueueSize);
        final Host[] hosts = Host.parseHosts(host, port);
        this.clientPolicy = new ClientPolicy();
        this.clientPolicy.maxConnsPerNode = Integer.parseInt(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.MAX_CONNECTIONS_PER_NODE, conf));
        this.clientPolicy.timeout = Integer.parseInt(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AEROSPIKE_TIMEOUT, conf));
        this.clientPolicy.eventLoops = this.eventLoops;
        this.client = new AerospikeClient(clientPolicy, hosts);
        GRAPH_ID = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_ID, conf);
        VERTEX_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.VERTEX_AERO_SET, conf);
        IN_VP_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.IN_VP_SET, conf);
        OUT_VP_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.OUT_VP_SET, conf);
        IN_IN_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.IN_IN_SET, conf);
        IN_OUT_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.IN_OUT_SET, conf);
        OUT_IN_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.OUT_IN_SET, conf);
        OUT_OUT_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.OUT_OUT_SET, conf);
        VERTEX_EDGELIST_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.VERTEX_EDGELIST_AERO_SET, conf);
        VERTEX_PROPERTY_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.VERTEX_PROPERTY_AERO_SET, conf);
        EDGE_ID_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_ID_KEY, conf);
        EDGE_ID_BIN = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_ID_BIN, conf);
        VERTEX_ID_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_ID_KEY, conf);
        VERTEX_ID_BIN = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_ID_BIN, conf);
        VERTEX_PROPERTY_ID_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_ID_KEY, conf);
        VERTEX_PROPERTY_ID_BIN = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_ID_BIN, conf);
        VERTEX_PROPERTY_NAME_TO_ID = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_NAME_TO_ID, conf);
        VERTEX_PROPERTY_NAME_TO_VALUE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_NAME_TO_VALUE, conf);
        VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, conf);
        VERTEX_PROPERTY_NAME = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_NAME, conf);
        EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, conf);
        PARENT_VERTEX_ID = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.PARENT_VERTEX_ID, conf);
        PROPERTIES = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.PROPERTIES, conf);
        VP_PROPERTIES = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VP_PROPERTIES, conf);
        TYPE_HINTS = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.TYPE_HINTS, conf);
        VP_TYPE_HINTS = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VP_TYPE_HINTS, conf);
        KEY_VALUE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.KEY_VALUE, conf);
        COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.COUNTER, conf);
        ID_MANAGER_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.ID_MANAGER_SET, conf);
        ID_TYPE_BIN = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ID_TYPE_BIN, conf);
        GLOBAL = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GLOBAL, conf);
        TEST_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.TEST_SET, conf);
        EDGE_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.EDGE_AERO_SET, conf);
        GRAPH_METADATA_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.GRAPH_METADATA_SET, conf);
        GRAPH_VARIABLES_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.GRAPH_VARIABLES_SET, conf);
        GRAPH_VARIABLES_RECORD = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_VARIABLES_RECORD, conf);
        GRAPH_VARIABLES_MAP = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_VARIABLES_MAP, conf);
        IN_EDGE_COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.IN_EDGE_COUNTER, conf);
        OUT_EDGE_COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.OUT_EDGE_COUNTER, conf);
        ID_CACHE_SIZE = Long.parseLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ID_CACHE_SIZE, conf));
        VP_COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VP_COUNTER, conf);
        INDEXED_BINS = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.INDEXED_BINS, conf);
        V_LABEL_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.V_LABEL_INDEX, conf));
        E_LABEL_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.E_LABEL_INDEX, conf));
        V_LABEL_INDEX_ENABLED = Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED, conf));
        E_LABEL_INDEX_ENABLED = Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED, conf));
        E_IN_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.E_IN_INDEX, conf));
        E_OUT_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.E_OUT_INDEX, conf));
        IN_EDGES = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.IN_EDGES, conf);
        OUT_EDGES = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.OUT_EDGES, conf);
        EDGE_CACHE_DISABLED = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_CACHE_DISABLED, conf);
        VP_CACHE_DISABLED = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VP_CACHE_DISABLED, conf);
        INDEX_METADATA = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.INDEX_METADATA, conf);
        RELATIONAL_VERTEX_TYPE_HINT = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.RELATIONAL_VERTEX_TYPE_HINT, conf);
        AEROSPIKE_CONNECTION_MAX_RETRY = Integer.parseInt(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AEROSPIKE_CONNECTION_MAX_RETRY, conf));
        CARDINALITY_METADATA_UPDATE_FREQUENCY = Long.parseLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.CARDINALITY_METADATA_UPDATE_FREQUENCY, conf));
        INDEX_METADATA_UPDATE_FREQUENCY = Long.parseLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.INDEX_METADATA_UPDATE_FREQUENCY, conf));
        USER_SUPPLIED_ID_CACHE_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_CACHE_SET, conf);
        USER_SUPPLIED_ID_VERTEX_CACHE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_VERTEX_CACHE, conf);
        USER_SUPPLIED_ID_EDGE_CACHE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_EDGE_CACHE, conf);
        USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE, conf);
        OPTIMIZED_TWO_HOP_STEPS = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.OPTIMIZED_TWO_HOP_STEPS, conf);
        OPTIMIZED_HOP_CONSTRAINT_STEPS = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.OPTIMIZED_HOP_CONSTRAINT_STEPS, conf);
        ADJACENCY_INDEX_ENABLED = Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ADJACENCY_INDEX_ENABLED, conf));
        EDGE_CACHE_DISABLED_GLOBALLY = Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_CACHE_DISABLED_GLOBALLY, conf));
        AEROSPIKE_BATCH_READ_SIZE = Integer.parseInt(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AEROSPIKE_BATCH_READ_SIZE, conf));
        FIREFLY_READ_THROUGH_CACHE_WEIGHT = Long.parseLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT, conf));
        cacheTasks = new ArrayList<>();
        idFactory = FireflyIdFactory.create(this);

        // Validate two hop steps.
        for (final String step : OPTIMIZED_TWO_HOP_STEPS) {
            if (!VALID_OPTIMIZED_TWO_HOP_STEPS.contains(step)) {
                LOG.error("Error starting graph with OPTIMIZED_TWO_HOP_STEPS {}. Valid values are {}.", OPTIMIZED_TWO_HOP_STEPS, VALID_OPTIMIZED_TWO_HOP_STEPS);
                throw new IllegalArgumentException("Error starting graph with OPTIMIZED_TWO_HOP_STEPS " + OPTIMIZED_TWO_HOP_STEPS +
                        ". Valid values are " + VALID_OPTIMIZED_TWO_HOP_STEPS + ".");
            }
        }

        // Validate hop constraint steps.
        for (final String step : OPTIMIZED_HOP_CONSTRAINT_STEPS) {
            if (!VALID_OPTIMIZED_HOP_CONSTRAINT_STEPS.contains(step)) {
                LOG.error("Error starting graph with OPTIMIZED_HOP_CONSTRAINT_STEPS {}. Valid values are {}.", OPTIMIZED_HOP_CONSTRAINT_STEPS, VALID_OPTIMIZED_HOP_CONSTRAINT_STEPS);
                throw new IllegalArgumentException("Error starting graph with OPTIMIZED_HOP_CONSTRAINT_STEPS " + OPTIMIZED_HOP_CONSTRAINT_STEPS +
                        ". Valid values are " + VALID_OPTIMIZED_HOP_CONSTRAINT_STEPS + ".");
            }
        }
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
        return scanHitCounter;
    }

    /**
     * Run a traversal prefetch task
     *
     * @param cacheId
     * @param task    prefetch task to execute
     */
    public void runPrefetchTask(UUID cacheId, Runnable task) {
        if (Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ASYNC_SUBGRAPH_CACHE, this.conf))) {
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

    /**
     * Connect to an Aerospike instance
     *
     * @param conf Apache Configuration
     * @return Database connection handle
     */
    public static AerospikeConnection connect(final Configuration conf) {
        return new AerospikeConnection(conf);
    }

    /**
     * Get a list of currently valid ids
     *
     * @param type type of Element
     * @return Iterator of raw Ids
     */
    public Iterator<FireflyId> readElementIds(final Class<? extends FireflyElement> type) {
        final AerospikeConnection.IdConfig cfg = new AerospikeConnection.IdConfig(this, type);
        return scanAllIdsInSet(cfg.getAeroSet());
    }

    /**
     * Return an iterator of all the ids in a set represented as FireflyId
     *
     * @param setName name of Aerospike set to scan
     * @return an Iterator of raw FireflyId
     */
    private Iterator<FireflyId> scanAllIdsInSet(final String setName) {
        final Class<? extends FireflyElement> type;
        if (setName.equals(VERTEX_AERO_SET)) {
            type = FireflyVertex.class;
        } else if (setName.equals(EDGE_AERO_SET)) {
            type = FireflyEdge.class;
        } else if (setName.equals(VERTEX_PROPERTY_AERO_SET)) {
            type = FireflyVertexProperty.class;
        } else {
            throw new IllegalArgumentException("Invalid set name: " + setName);
        }

        LOG.trace("Scanning {} ids.", setName);
        final Iterator<Map.Entry<Key, Record>> i = scanAllKeysInSet(setName, null);
        return IteratorUtils.map(i,
                r -> idFactory.createFromRecord(this, FireflyRecord.fromRecord(this, r.getKey(), r.getValue()), type));
    }

    public Iterator<Map.Entry<Key, Record>> scanAllKeysInSet(final String setName, final Expression exp, String... binNames) {
        LOG.trace("Scanning all ids in {}:{} with filter {}.", setName, Arrays.toString(binNames), exp);
        ScanPolicy policy = new ScanPolicy();
        policy.includeBinData = false;
        return scanAllRecordsInSet(setName, exp, policy, binNames);
    }

    /**
     * Issue a scan query for all the records in a set.
     * Filter by an Exp, provide a ScanPolicy, optionally provide binNames to return
     * Note - if the client or the event loop was closed prior to this, this function will hang indefinitely.
     *
     * @param setName  Aerospike set name to scan
     * @param exp      Expression to apply to Scan
     * @param policy   ScanPolicy to use during Scan
     * @param binNames Bin names to read into Records returned
     * @return Iterator of Map.Entry Key, Record
     */
    public Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName, final Expression exp, ScanPolicy policy, String... binNames) {
        LOG.debug("Issuing scan query of all records in {}:{}:{} with filter {}.", getNamespace(), setName, Arrays.toString(binNames), exp);
        final Throttles throttles = new Throttles(getEventLoops().getSize(), getCommandsPerLoop());
        final Monitor scanMonitor = new Monitor();
        final int progressFreq = 100;
        policy.sendKey = true;
        if (exp != null) policy.filterExp = exp;

        final ConcurrentScanRecordSequenceListener listener = new ConcurrentScanRecordSequenceListener(
                scanMonitor,
                Integer.parseInt(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.SCAN_MAX_WAIT, conf)));
        client.scanAll(getEventLoops().next(), listener, policy, getNamespace(), setName, binNames);
        return listener.iterator();
    }

    public EventLoops getEventLoops() {
        return eventLoops;
    }

    public int getCommandsPerLoop() {
        return commandsPerLoop;
    }

    /**
     * Given an array of edge Records, and a direction, return an array of the Vertex Records they are linking to
     *
     * @param edgeRecords array of Edge Records
     * @param direction   the other end we should be retrieving
     * @return an array of Vertex records
     */
    public List<KeyRecord> vertexRecordsFromEdgeRecords(Record[] edgeRecords, Direction direction) {
        List<Key> vertexKeys = Arrays.stream(edgeRecords)
                .map(record -> record.getString(direction.name()))
                .map(hash -> new Key(namespace, FireflyIdPoly.decodeBase64(hash), VERTEX_AERO_SET, Value.NULL))
                .collect(Collectors.toList());
        Record[] vertexRecords = read(vertexKeys.toArray(new Key[]{}));
        List<KeyRecord> krl = new ArrayList<>();
        IntStream.range(0, vertexKeys.size()).forEach(i -> {
            krl.add(new KeyRecord(vertexKeys.get(i), vertexRecords[i]));
        });
        return krl;
    }

    public ComparableVersion getDataModelVerion() {
        final Key k = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        Record dataModelRec = read(k, AerospikeConnection.noSendKeyReadPolicy);
        if (dataModelRec == null)
            return null;
        return new ComparableVersion(dataModelRec.getString(DATA_MODEL_VER));
    }

    public void setModelVersion(final String ver) {
        final Key k = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        final Bin b = new Bin(DATA_MODEL_VER, ver);
        write(k, b);
    }

    public String getDataModelName() {
        final Key k = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        final Record dataModelRec = read(k, AerospikeConnection.noSendKeyReadPolicy);
        if (dataModelRec == null)
            return null;
        return dataModelRec.getString(DATA_MODEL_NAME);
    }

    public void setModelName(final String name) {
        final Key k = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        final Bin b = new Bin(DATA_MODEL_NAME, name);
        write(k, b);
    }


    /**
     * manage the set names for an element type
     */
    public static class IdConfig {
        private final Class<? extends FireflyElement> type;
        private final String AERO_SET;
        private final String ID_KEY;
        private final String ID_BIN;

        public IdConfig(AerospikeConnection ac, final Class<? extends FireflyElement> type) {
            this.type = type;
            if (type == FireflyVertex.class) {
                AERO_SET = ac.VERTEX_AERO_SET;
                ID_KEY = ac.VERTEX_ID_KEY;
                ID_BIN = ac.VERTEX_ID_BIN;
            } else if (type == FireflyEdge.class) {
                AERO_SET = ac.EDGE_AERO_SET;
                ID_KEY = ac.EDGE_ID_KEY;
                ID_BIN = ac.EDGE_ID_BIN;
            } else if (type == LinkedVertexProperty.class) {
                AERO_SET = ac.VERTEX_PROPERTY_AERO_SET;
                ID_KEY = ac.VERTEX_PROPERTY_ID_KEY;
                ID_BIN = ac.VERTEX_PROPERTY_ID_BIN;
            } else
                throw new RuntimeException("Unknown ID type: " + type);
        }

        String getIdKey() {
            return ID_KEY;
        }

        public String getAeroSet() {
            return AERO_SET;
        }

        String getIdBin() {
            return ID_BIN;
        }
    }

    public static class InfoOps {
        protected static class Keys {
            public static final String SET = "set";
            public static final String SETS = "sets";
            public static final String NS = "ns";
            public static final String OBJECTS = "objects";
            public static final String SINDEX = "sindex";
            public static final String SINDEX_LIST = "sindex-list";
            public static final String FEATURE_KEY = "feature-key";
            public static final String INDEXNAME = "indexname";
            public static final String RESULT = "result";
        }

        //Parse the whole infoResponse and return it as a List of Maps
        protected static List<Map<String, String>> parseRaw(String infoResponse) {
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

        //
        private static Map<String, Map<String, String>> parseBySet(String infoResponse, String namespace) {
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
            final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], Keys.SINDEX);
            List<Map<String, String>> res = parseRaw(infoResponse);
            return res.stream()
                    .filter(m -> m.get(Keys.NS).equals(namespace))
                    .map(m -> {
                        return (Map.Entry<String, String>) new AbstractMap.SimpleEntry(m.get(Keys.INDEXNAME), m.get(Keys.SET));
                    }).collect(Collectors.toList());
        }

        /**
         * Is the first connected Aerospike instance "Enterprise Edition"
         *
         * @param client AerospikeClient connection instance
         * @return enterprise or not
         */
        public static boolean isEnterprise(AerospikeClient client) {
            final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], Keys.FEATURE_KEY);
            return (infoResponse != null && !infoResponse.isEmpty());
        }

        /**
         * Get the number of Records in a Set for a particular namespace
         *
         * @param setName   Name of set to query for number of records
         * @param namespace Namespace containing set
         * @param client    AerospikeClient instance
         * @return Number of Records in set
         */
        public static long getSetSize(final String setName, String namespace, AerospikeClient client) {
            if (client.getNodes().length > 1)
                throw new RuntimeException("getSetSize not supported for multi node");
            final String infoQuery = "sets/" + namespace + "/" + setName;
            final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], infoQuery);
            final List<Long> setSize = Arrays.stream(infoResponse.split(":"))
                    .filter(str -> str.startsWith("objects"))
                    .map(str -> Long.valueOf(str.split("=")[1]))
                    .collect(Collectors.toList());
            return setSize.isEmpty() ? 0 : setSize.get(0);
        }

        /**
         * Get a list of all the Sets in a namespace
         *
         * @param namespace namespace to query
         * @param client    AerospikeClient instance
         * @return Set of namespaces
         */
        public static Set<String> getSetList(String namespace, AerospikeClient client) {
            String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], Keys.SETS);
            final Set<String> allSets = parseBySet(infoResponse, namespace).keySet();
            return allSets;
        }

        /**
         * Get a list of all the Sets in a namespace that have a number of records > 0
         *
         * @param namespace namespace to query
         * @param client    AerospikeClient instance
         * @return Set of namespaces
         */
        public static Set<String> getNonEmptySetList(String namespace, AerospikeClient client) {
            final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], Keys.SETS);
            return parseBySet(infoResponse, namespace).entrySet().stream().filter(entry -> {
                        Map<String, String> map = entry.getValue();
                        return Integer.parseInt(map.get(Keys.OBJECTS)) > 0;
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new))
                    .keySet();
        }
    }

    public static final Map<Class<? extends Serializable>, Class<? extends Serializable>> KeyToDiskTypeMap = new HashMap<>() {{
        put(Long.class, Long.class);
        put(Integer.class, Long.class);
        put(Double.class, Double.class);
        put(byte[].class, byte[].class);
        put(String.class, String.class);
    }};
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

    private final int commandsPerLoop = 25;
    private final ClientPolicy clientPolicy;
    static AtomicLong readMetric = new AtomicLong(0);
    static AtomicLong writeMetric = new AtomicLong(0);
    static AtomicLong generationCheckRetryMetric = new AtomicLong(0);
    static AtomicLong generationCheckHighWaterMark = new AtomicLong(0);

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
        else if (LinkedVertexProperty.class.isAssignableFrom(type))
            return VERTEX_PROPERTY_AERO_SET;
        else if (FireflyVertexProperty.class.isAssignableFrom(type)) {
            return VERTEX_AERO_SET;
        }
        throw new UnsupportedOperationException("Element not supported " + type.getName());
    }

    /**
     * Return the numeric id of the on-disk type
     *
     * @param clazz class to lookup
     * @return index of supported type
     */
    public static Long getSupportedType(final Class clazz) {
        if (!SupportedValueTypes.containsKey(clazz))
            throw new UnsupportedOperationException(clazz.getName() + " is not a supported value type");
        return SupportedValueTypes.get(clazz);
    }

    /**
     * Create Indexes for Firefly
     */
    public void createGraphIndexes() {
        LOG.info("Creating graph indices.");
        List<String> existingIndexes =
                InfoOps.listExistingIndexes(getClient(), getNamespace()).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());
        if (ADJACENCY_INDEX_ENABLED) {
            createIndex(existingIndexes, setFromElementType(FireflyEdge.class),
                    E_IN_INDEX, Direction.IN.name(),
                    IndexType.STRING, IndexCollectionType.DEFAULT);
            createIndex(existingIndexes, setFromElementType(FireflyEdge.class),
                    E_OUT_INDEX, Direction.OUT.name(),
                    IndexType.STRING, IndexCollectionType.DEFAULT);
        }

        if (V_LABEL_INDEX_ENABLED) {
            createIndex(existingIndexes, setFromElementType(FireflyVertex.class),
                    V_LABEL_INDEX, LABEL, IndexType.STRING, IndexCollectionType.DEFAULT);
        }
        if (E_LABEL_INDEX_ENABLED) {
            createIndex(existingIndexes, setFromElementType(FireflyEdge.class),
                    E_LABEL_INDEX, LABEL, IndexType.STRING, IndexCollectionType.DEFAULT);
        }
    }

    /**
     * Drop indices for Firefly
     */
    public void dropGraphIndices() {
        LOG.debug("Dropping graph indices.");
        dropIndex(setFromElementType(FireflyVertex.class), E_IN_INDEX);
        dropIndex(setFromElementType(FireflyEdge.class), E_OUT_INDEX);
        dropIndex(setFromElementType(FireflyVertex.class), V_LABEL_INDEX);
        dropIndex(setFromElementType(FireflyEdge.class), E_LABEL_INDEX);
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
    private EventLoops initializeEventLoops(
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
    protected Record read(final Key key, final Policy policy) {
        readMetric.addAndGet(1);
        final FireflyCache cache = transactionCache.get();
        final Record[] results;
        try { //@todo policy causes key mismatch error
            results = (cache != null) ? cache.read(new Key[]{key}) : new Record[]{client.get(policy, key)};
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
    protected Record[] read(final Key[] keys) {
        return read(keys, AerospikeConnection.noSendKeyBatchPolicy);
    }

    /**
     * Perform a batch Aerospike read for a group of keys
     *
     * @param keys        Array of Key to return records for
     * @param batchPolicy BatchPolicy to use
     * @return Array of Record
     */
    protected Record[] read(final Key[] keys, final BatchPolicy batchPolicy) {
        readMetric.addAndGet(keys.length);
        final FireflyCache cache = transactionCache.get();
        final Record[] results;
        try { //@todo policy causes key mismatch error
            results = (cache != null) ? cache.read(keys) : client.get(batchPolicy, keys);
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
    protected void write(final Key key, final Bin... bins) {
        Bin[] newBins;
        //@todo This is a temporary measure to pack the user key into a bin.
        //@todo Remove when sendKey works to recover the user key for hash constructed keys
        if (key.userKey.getObject() != null) {
            newBins = Arrays.copyOf(bins, bins.length + 1);
            newBins[bins.length] = new Bin(USER_KEY, key.userKey.getObject());
        } else {
            newBins = bins;
        }
        write(key, -1, newBins);
    }

    /**
     * Write to Aerospike, notify the cache implementation
     *
     * @param key  Key to write Bins into
     * @param bins Data Bin(s) to write
     */
    protected void write(final Key key, final int generation, final Bin... bins) {
        Bin[] newBins;
        //@todo This is a temporary measure to pack the user key into a bin.
        //@todo Remove when sendKey works to recover the user key for hash constructed keys
        if (key.userKey.getObject() != null) {
            newBins = Arrays.copyOf(bins, bins.length + 1);
            newBins[bins.length] = new Bin(USER_KEY, key.userKey.getObject());
        } else {
            newBins = bins;
        }

        writeMetric.incrementAndGet();
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        writePolicy.maxRetries = AEROSPIKE_CONNECTION_MAX_RETRY;
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
     */
    public void delete(final Key key) {
        final FireflyCache cache = transactionCache.get();
        if (cache != null) {
            cache.invalidate(key);
        }
        client.delete(null, key);
    }

    /**
     * query if the connected Aerospike instance is licenced for Enterprise Edition
     *
     * @return connected Aerospike instance is Enterprise Edition
     */
    public boolean isEnterprise() {
        return InfoOps.isEnterprise(client);
    }


    public Iterator<KeyRecord> queryIndex(final String setName, final String indexName, final Filter filter) {
        return queryIndex(setName, indexName, filter, new QueryPolicy());
    }

    /**
     * Issue a query on an index providing a custom filter
     *
     * @param setName   Name of Aerospike set
     * @param indexName Name of Index to query
     * @param filter    Custom Filter
     * @return Iterator of KeyRecord pair results
     */
    public Iterator<KeyRecord> queryIndex(String setName, String indexName, Filter filter, QueryPolicy policy) {
        final Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setName);
        stmt.setIndexName(indexName);
        stmt.setFilter(filter);
        return client.query(policy, stmt).iterator();
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
     * Increment the generation check retry count
     */
    public static void incrementGenerationCheckRetryMetric() {
        generationCheckRetryMetric.incrementAndGet();
    }

    /**
     * Return the generation check retry count
     *
     * @return generation check retry count
     */
    public static long getGenerationCheckRetryMetric() {
        return generationCheckRetryMetric.get();
    }


    /**
     * Set the generation check high-water mark
     */
    public static void setGenerationCheckHighWaterMark(final long value) {
        generationCheckRetryMetric.updateAndGet(x -> Math.max(x, value));
    }

    /**
     * Return the high-water mark for generation check retries
     *
     * @return number of retries
     */
    public static long getGenerationCheckHighWaterMark() {
        return generationCheckHighWaterMark.get();
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
                                            final String mapKey,
                                            final String typeHintBin) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, fid);
        if (fireflyRecord == null || fireflyRecord.record == null ||
                fireflyRecord.record.getMap(mapName) == null ||
                !fireflyRecord.record.getMap(mapName).containsKey(mapKey))
            return null;
        final Object value = fireflyRecord.record.getMap(mapName).get(mapKey);
        final Long typeHint = (Long) fireflyRecord.record.getMap(typeHintBin).get(mapKey);
        final Class valueClass = SupportedTypeValues.get(typeHint);
        return (V) typeCast(valueClass, value);
    }

    /**
     * Return the first key-value from a map
     * read its associated type-hint and reconstruct the correct JVM type for the value
     *
     * @param aeroSet
     * @param fid
     * @param mapName
     * @param <V>
     * @return
     */
    public <V> AbstractMap.Entry<String, V> readTypeHintedKeyValueFromMap(final String aeroSet,
                                                                          final FireflyId fid,
                                                                          final String mapName,
                                                                          final String typeHintBin) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, fid);
        if (fireflyRecord == null || fireflyRecord.record == null || fireflyRecord.record.getMap(mapName).size() == 0)
            return null;
        final Optional<? extends Map<?, ?>> map = Optional.ofNullable(fireflyRecord.record.getMap(mapName));
        if (!map.isPresent())
            return null;
        final String mapKey = (String) map.get().keySet().iterator().next();
        final Long typeHint = (Long) fireflyRecord.record.getMap(typeHintBin).get(mapKey);
        final Object val = map.get().values().iterator().next();

        if (val == null)
            return null;
        final Class clazz = SupportedTypeValues.get(typeHint);
        return new AbstractMap.SimpleEntry<>(mapKey, (V) typeCast(clazz, val));
    }

    public Object convertValuetoTypeUsingHint(final Object value, final Long typeHint) {
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
     * Write a key-value pair into a Map on a Record.
     * Also store a type hint so it can be reconstructed as the correct type.
     * Pass additional Bins so 1 write can be made.
     * UPDATE_ONLY write policy to prevent unintended writes to concurrently deleted elements.
     *
     * @param aeroSet
     * @param fid
     * @param mapName
     * @param mapKey
     * @param value
     * @param typeHintBinName
     * @param additionalBins
     * @param <V>
     */
    public <V> void writeTypeHintedValueToMap(final String aeroSet,
                                              final FireflyId fid,
                                              final String mapName,
                                              final String mapKey,
                                              final V value,
                                              final String typeHintBinName,
                                              final Bin... additionalBins) {
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        writeTypeHintedValueToMapWithPolicy(aeroSet, fid, mapName, mapKey, value, typeHintBinName, writePolicy, additionalBins);
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

    private <V> void writeTypeHintedValueToMapWithPolicy(final String aeroSet,
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
            typeHintOp = MapOperation.removeByKey(typeHintBinName, Value.get(mapKey), MapReturnType.NONE);
        } else {
            final MapPolicy policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
            valueOp = MapOperation.put(policy, mapName, Value.get(mapKey), Value.get(value));
            typeHintOp = MapOperation.put(policy, typeHintBinName, Value.get(mapKey),
                    Value.get(getSupportedType(value.getClass())));
        }
        final Expression idTypeExp = Exp.build(Exp.val(fid.getStorageTypeHint()));
        final Operation idTypeOp = ExpOperation.write(this.ID_TYPE_BIN, idTypeExp,
                ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL);
        ops.add(valueOp);
        ops.add(typeHintOp);
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
     * get the current value of an Id counter
     *
     * @param name name of Counter
     * @return value of counter
     */
    public long getIdCounter(final String name) {
        final Record record = read(new Key(namespace, ID_MANAGER_SET, name), AerospikeConnection.noSendKeyReadPolicy);
        return record.getLong(COUNTER);
    }

    /**
     * Increment an Id counter by a suppled value and return its incremented value
     *
     * @param name      name of Counter to operate on
     * @param increment value to increment by
     * @return value of counter after operation
     */
    public long incrementAndGetIdCounter(final String name, long increment) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, increment);
        final Record record = this.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return record.getLong(COUNTER);
    }

    /**
     * Increment an Id counter by 1 and return its incremented value
     *
     * @param name name of Counter to operate on
     * @return value of Counter after operation
     */
    public long incrementAndGetIdCounter(final String name) {
        return incrementAndGetIdCounter(name, 1);
    }

    /**
     * Decrement an Id counter by 1
     *
     * @param name of Counter to operate on
     * @return value of counter after operation
     */
    public long decrementIdCounter(final String name) {
        return decrementIdCounter(name, 1L);
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
        final Bin ctr = new Bin(COUNTER, -amount);
        final Record record = this.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return record.getLong(COUNTER);
    }

    /**
     * Zero an Id counter
     *
     * @param name name of Counter to operate on
     * @return value of counter after operation
     */
    public long zeroIdCounter(final String name) {
        final Bin ctr = new Bin(COUNTER, 0);
        FireflyRecord.writeElement(this, ID_MANAGER_SET, FireflyIdPoly.fromObject(name, ID_MANAGER_SET), -1, ctr);
        return 0L;
    }

    /**
     * Offer a value, compare it to the current counter value.
     * if the offered value is greater than the current counter value
     * set the counter to the offered value, and return it.
     * otherwise, increment the counter by 1, and return that.
     *
     * @param offer proposed value
     * @param name  name of counter
     * @return Incremented counter value or offered value
     */

    public long greaterOrIncrement(final long offer, final String name) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        Expression gtexp = Exp.build(Exp.cond(
                Exp.gt(
                        Exp.val(offer),
                        Exp.add(Exp.intBin(COUNTER), Exp.val(1))
                ),
                Exp.val(offer),
                Exp.add(Exp.intBin(COUNTER), Exp.val(1))
        ));
        Record result = this.operate(null, key, ExpOperation.write(COUNTER, gtexp, ExpWriteFlags.DEFAULT), Operation.get(COUNTER));
        ArrayList<Object> ret = (ArrayList<Object>) result.getValue(COUNTER);
        return (long) ret.get(1);
    }

    /**
     * over a value and name a counter. return the greater of the two.
     *
     * @param offer proposed value
     * @param name  name of counter to operate on
     * @return value of counter or proposed value
     */
    public long greaterOrExisting(final long offer, final String name) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        Expression gtexp = Exp.build(Exp.cond(
                Exp.gt(
                        Exp.val(offer),
                        Exp.add(Exp.intBin(COUNTER), Exp.val(1))
                ),
                Exp.val(offer),
                Exp.intBin(COUNTER)
        ));
        Record result = this.operate(null, key, ExpOperation.write(COUNTER, gtexp, ExpWriteFlags.DEFAULT), Operation.get(COUNTER));
        ArrayList<Object> ret = (ArrayList<Object>) result.getValue(COUNTER);
        return (long) ret.get(1);
    }

    /**
     * Drop data in all Aerospike sets associated with currently configured graph by issuing a Truncate operation
     *
     * @param dropIndices drop graph indices
     */
    public void dropDatabase(boolean dropIndices) {
        LOG.info("Dropping database.");
        try {
            // If using the client APIs to perform the truncate command on a single-threaded application it is
            // suggested to add a millisecond (ms) sleep. The truncate operation has a 1 millisecond resolution and
            // writes occurring within the same millisecond are not deleted.
            // Source: https://discuss.aerospike.com/t/guidelines-for-deleting-data/3681/1
            Thread.sleep(1);
            client.truncate(null, namespace, EDGE_AERO_SET, null);
            client.truncate(null, namespace, VERTEX_AERO_SET, null);
            client.truncate(null, namespace, VERTEX_PROPERTY_AERO_SET, null);
            client.truncate(null, namespace, ID_MANAGER_SET, null);
            client.truncate(null, namespace, USER_SUPPLIED_ID_CACHE_SET, null);
            client.truncate(null, namespace, TEST_SET, null);
            client.truncate(null, namespace, VERTEX_EDGELIST_AERO_SET, null);
            client.truncate(null, namespace, GRAPH_VARIABLES_SET, null);
            client.truncate(null, namespace, GRAPH_METADATA_SET, null);
            client.truncate(null, namespace, INDEX_METADATA, null);
            client.truncate(null, namespace, OUT_VP_SET, null);
            client.truncate(null, namespace, IN_VP_SET, null);
            client.truncate(null, namespace, OUT_OUT_SET, null);
            client.truncate(null, namespace, OUT_IN_SET, null);
            client.truncate(null, namespace, IN_OUT_SET, null);
            client.truncate(null, namespace, IN_IN_SET, null);
            if (dropIndices)
                dropGraphIndices();
            Thread.sleep(1);
        } catch (final InterruptedException e) {
            // Why would anyone invoke this method in a runner thread that can also have interrupt() called on it? Who
            // knows - just be amazed that they did it with 1ms precision and handle it anyway.
            LOG.warn("InterruptedException caught during database truncate: ", e);
            Thread.currentThread().interrupt();
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
     * Drop database by truncate, do not drop indices
     */
    public void dropDatabase() {
        dropDatabase(false);
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

    public String getVpIndexPrefix() {
        return String.format("%s_%s", GRAPH_ID, VP_INDEX_PREFIX);
    }

    public String getEpIndexPrefix() {
        return String.format("%s_%s", GRAPH_ID, EP_INDEX_PREFIX);
    }

    /**
     * Create an Aerospike Index.
     *
     * @param existingIndexes
     * @param set                 Set name
     * @param indexName           Index name
     * @param binName             Bin name to be indexed
     * @param keyName             Key of map to create sindex on
     * @param type                Index type
     * @param indexCollectionType Index Collection Type
     */
    public void createKeyValueSindex(
            final List<String> existingIndexes,
            final String set,
            final String indexName,
            final String binName,
            final String keyName,
            final IndexType type,
            final IndexCollectionType indexCollectionType
    ) {
        if (existingIndexes.contains(indexName)) {
            LOG.debug("Index {} already exists", indexName);
            return;
        } else {
            LOG.info("Creating index {}:{}:{}.", set, indexName, binName);
        }
        final Policy policy = new Policy();
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            final CTX ctx = CTX.mapKey(Value.get(keyName));
            final IndexTask task = client.createIndex(policy, namespace, set, indexName, binName, type, indexCollectionType, ctx);
            task.waitTillComplete(1);
            LOG.debug("Index {} creation completed.", indexName);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    /**
     * Create an Index on a particular Bin
     *
     * @param indexClass          Firefly Element Class
     * @param binName             Name of Bin
     * @param indexType           Type of Index
     * @param indexCollectionType Type of Index Collection
     * @param <T>                 FireflyElement Type
     */
    public <T extends Element> void createBinIndex(Class<? extends FireflyElement> indexClass,
                                                   String binName,
                                                   IndexType indexType,
                                                   IndexCollectionType indexCollectionType) {
        Key mKey = new Key(namespace, INDEX_METADATA, setFromElementType(indexClass));
        Record rec = read(mKey, AerospikeConnection.noSendKeyReadPolicy);

        List<String> keys = rec == null ? new ArrayList<>() : (List<String>) rec.getList(INDEXED_BINS);
        keys.add(binName);
        Bin keysBin = new Bin(INDEXED_BINS, new ArrayList<>(new HashSet<>(keys)));
        checkedPut(null, mKey, keysBin);
        createIndex(new ArrayList<>(), setFromElementType(indexClass), binName, binName, indexType, indexCollectionType);
    }

    /**
     * @param indexClass
     * @param key
     * @param <T>
     */
    public <T extends Element> void dropBinIndex(Class<? extends FireflyElement> indexClass, String key) {
        Key mKey = new Key(namespace, INDEX_METADATA, setFromElementType(indexClass));
        Record rec = read(mKey, AerospikeConnection.noSendKeyReadPolicy);
        List<String> keys = (List<String>) rec.getList(INDEXED_BINS);
        keys.remove(key);
        Bin keysBin = new Bin(INDEXED_BINS, new ArrayList<>(new HashSet<>(keys)));
        checkedPut(null, mKey, keysBin);
        dropIndex(setFromElementType(indexClass), key);
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
        try {
            return this.getClient().operate(writePolicy, key, operations);
        } catch (final AerospikeException ae) {
            switch (ae.getResultCode()) {
                case ResultCode.RECORD_TOO_BIG:
                    LOG.error(RECORD_TOO_BIG, ae);
                    throw new RuntimeException(RECORD_TOO_BIG, ae);
                case ResultCode.KEY_NOT_FOUND_ERROR:
                    LOG.error(ELEMENT_NOT_FOUND, ae);
                    throw new RuntimeException(ELEMENT_NOT_FOUND, ae);
                default:
                    LOG.error(ae.getMessage());
                    throw ae;
            }
        }
    }

    @Override
    public final String toString() {
        return String.format("aerospike://%s:%s/%s", host, port, namespace);
    }

    /**
     * close the connection to Aerospike
     */
    public void close() {
        LOG.debug("Closing client.");
        this.client.close();
        LOG.debug("Closing event loop.");
        this.eventLoops.close();
    }
}
