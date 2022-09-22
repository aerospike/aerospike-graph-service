package com.aerospike.firefly.io;

import com.aerospike.client.*;
import com.aerospike.client.async.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.*;
import com.aerospike.client.task.IndexTask;
import com.aerospike.firefly.io.impl.TraversalCache;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.util.ConfigurationHelper;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.configuration2.Configuration;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.process.traversal.strategy.optimization.FireflyTraversalCacheStrategy.Util.isCachedTraversal;
import static com.aerospike.firefly.process.traversal.strategy.optimization.FireflyTraversalCacheStrategy.Util.idFromTraversal;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class AerospikeConnection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeConnection.class);
    public static final String LABEL = "label";

    private static final WritePolicy sendKeyWritePolicy = new WritePolicy();
    private static final String DATA_MODEL_KEY = "DATA_MODEL_KEY";
    private static final String DATA_MODEL_NAME = "DATA_MODEL_NAME";
    private static final String DATA_MODEL_VER = "DATA_MODEL_VER";

    static {
        sendKeyWritePolicy.sendKey = true;
    }

    public final String GRAPH_ID;

    public final String NUMERIC_VP_KV_INDEX;
    public final String STRING_VP_KV_INDEX;
    public final String NUMERIC_V_VP_KV_INDEX;
    public final String STRING_V_VP_KV_INDEX;
    public final String STRING_E_KV_INDEX;
    public final String NUMERIC_E_KV_INDEX;
    private final String INDEXED_BINS;
    public final String V_LABEL_INDEX;
    public final String E_LABEL_INDEX;
    private final String E_IN_INDEX;
    private final String E_OUT_INDEX;


    private static final boolean SUPERNODE_INDEX_ENABLED = false;

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
    public final String CACHE_DISABLED;
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
    protected final String EDGE_PROPERTIES;
    protected final String VP_PROPERTIES;
    public final String TYPE_HINTS;
    public final String KEY_VALUE;
    protected final String COUNTER;
    protected final String ID_MANAGER_SET;
    public final String ID_TYPE;
    public final String GLOBAL;
    public final String TEST_SET;
    public final Configuration conf;

    // User supplied id cache
    public final String USER_SUPPLIED_ID_CACHE_SET;
    public final String USER_SUPPLIED_ID_VERTEX_CACHE;
    public final String USER_SUPPLIED_ID_EDGE_CACHE;
    public final String USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE;
    public final ConcurrentHashMap<UUID, TraversalCache> traversalCacheSet;
    public final List<AbstractMap.Entry<UUID, CompletableFuture<Void>>> cacheTasks;

    public final ThreadLocal<Traversal.Admin> currentTraversal = new ThreadLocal<>();

    /**
     * Construct a new AerospikeConnection
     *
     * @param conf Apache Configuration
     */
    public AerospikeConnection(final Configuration conf) {
        LOG.info("Initializing AerospikeConnection.");
        LOG.debug("CONFIGURATION:");
        conf.getKeys().forEachRemaining(key -> {
            LOG.debug(String.format("\tconfig: [%s]:[%s]", key, conf.get(String.class, key)));
        });
        LOG.debug("\thost {} {}", ConfigurationHelper.Keys.AEROSPIKE_HOST, conf.get(String.class, ConfigurationHelper.Keys.AEROSPIKE_HOST));
        LOG.debug("\tport {} {}", ConfigurationHelper.Keys.AEROSPIKE_PORT, conf.get(Integer.class, ConfigurationHelper.Keys.AEROSPIKE_PORT));
        LOG.debug("\tns {} {}", ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, conf.get(String.class, ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE));
        this.conf = conf;
        this.host = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AEROSPIKE_HOST, conf);
        this.port = Integer.valueOf(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AEROSPIKE_PORT, conf));
        this.namespace = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, conf);

        this.eventLoops = initializeEventLoops(EventLoopType.DIRECT_NIO, NumLoops, CommandsPerEventLoop, DelayQueueSize);
        final Host[] hosts = Host.parseHosts(host, port);
        this.clientPolicy = new ClientPolicy();
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
        EDGE_PROPERTIES = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_PROPERTIES, conf);
        VP_PROPERTIES = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VP_PROPERTIES, conf);
        TYPE_HINTS = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.TYPE_HINTS, conf);
        KEY_VALUE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.KEY_VALUE, conf);
        COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.COUNTER, conf);
        ID_MANAGER_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.ID_MANAGER_SET, conf);
        ID_TYPE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ID_TYPE, conf);
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
        NUMERIC_VP_KV_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.NUMERIC_VP_KV_INDEX, conf));
        STRING_VP_KV_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.STRING_VP_KV_INDEX, conf));
        NUMERIC_V_VP_KV_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.NUMERIC_V_VP_KV_INDEX, conf));
        STRING_V_VP_KV_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.STRING_V_VP_KV_INDEX, conf));
        STRING_E_KV_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.STRING_E_KV_INDEX, conf));
        NUMERIC_E_KV_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.NUMERIC_E_KV_INDEX, conf));
        INDEXED_BINS = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.INDEXED_BINS, conf);
        V_LABEL_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.V_LABEL_INDEX, conf));
        E_LABEL_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.E_LABEL_INDEX, conf));
        E_IN_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.E_IN_INDEX, conf));
        E_OUT_INDEX = String.format("%s_%s", GRAPH_ID, ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.E_OUT_INDEX, conf));
        IN_EDGES = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.IN_EDGES, conf);
        OUT_EDGES = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.OUT_EDGES, conf);
        CACHE_DISABLED = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.CACHE_DISABLED, conf);
        INDEX_METADATA = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.INDEX_METADATA, conf);
        RELATIONAL_VERTEX_TYPE_HINT = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.RELATIONAL_VERTEX_TYPE_HINT, conf);


        // User supplied id cache.
        USER_SUPPLIED_ID_CACHE_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_CACHE_SET, conf);
        USER_SUPPLIED_ID_VERTEX_CACHE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_VERTEX_CACHE, conf);
        USER_SUPPLIED_ID_EDGE_CACHE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_EDGE_CACHE, conf);
        USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE, conf);


        traversalCacheSet = new ConcurrentHashMap<>();
        cacheTasks = new ArrayList<>();
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
    public Iterator<?> readElementIds(final Class<? extends FireflyElement> type) {
        final AerospikeConnection.IdConfig cfg = new AerospikeConnection.IdConfig(this, type);
        return scanAllIdsInSet(cfg.getAeroSet());
    }

    /**
     * Return an iterator of all the (raw) ids in a set
     *
     * @param setName name of Aerospike set to scan
     * @return an Iterator of raw Long id values
     */
    private Iterator<Long> scanAllIdsInSet(final String setName) {
        //@todo performance
        LOG.trace("Scanning {} ids.", setName);
        final Iterator<Map.Entry<Key, Record>> i = scanAllKeysInSet(setName, null);
        return IteratorUtils.map(i, keyRecordEntry -> NumericIdManager.convert(keyRecordEntry.getKey().userKey.getObject()));
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
        LOG.trace("Issuing scan query of all records in {}:{}:{} with filter {}.", getNamespace(), setName, Arrays.toString(binNames), exp);
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
                .map(record -> record.getLong(direction.name()))
                .map(id -> new Key(namespace, VERTEX_AERO_SET, id)).collect(Collectors.toList());
        Record[] vertexRecords = read(vertexKeys.toArray(new Key[]{}));
        List<KeyRecord> krl = new ArrayList<>();
        IntStream.range(0, vertexKeys.size()).forEach(i -> {
            krl.add(new KeyRecord(vertexKeys.get(i), vertexRecords[i]));
        });
        return krl;
    }

    public ComparableVersion getModelVersion() {
        final Key k = new Key(namespace, GRAPH_METADATA_SET, DATA_MODEL_KEY);
        Record dataModelRec = read(k);
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
        final Record dataModelRec = read(k);
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
            } else if (type == FireflyVertexProperty.class) {
                AERO_SET = ac.VERTEX_PROPERTY_AERO_SET;
                ID_KEY = ac.VERTEX_PROPERTY_ID_KEY;
                ID_BIN = ac.VERTEX_PROPERTY_ID_BIN;
            } else
                throw new RuntimeException("unknown id type: " + type);
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
                        Map<String, String> data = new HashMap<>();
                        Arrays.stream(strAry).forEach(entryStr -> {
                            if(entryStr.isEmpty())
                                return;
                            if(entryStr.contains("="))
                                data.put(entryStr.split("=")[0], entryStr.split("=")[1]);
                            else
                                data.put(Keys.RESULT,entryStr);
                        });
                        if(data.size()>0)
                            results.add(data);
                    });
            return results;
        }

        //
        private static Map<String, Map<String, String>> parseBySet(String infoResponse, String namespace) {
            Map<String, Map<String, String>> results = new HashMap<>();
            Arrays.stream(infoResponse.split(";"))
                    .filter(str -> str.startsWith(Keys.NS + "=" + namespace))
                    .map(str -> str.split(":"))
                    .forEach(strAry -> {
                        Map<String, String> data = new HashMap<>();
                        Arrays.stream(strAry).forEach(kvStr -> {
                            data.put(kvStr.split("=")[0], kvStr.split("=")[1]);
                        });
                        results.put(data.get(Keys.SET), data);
                    });
            return results;
        }

        public static List<String> listExistingIndexes(final AerospikeClient client, final String namespace) {
            final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], Keys.SINDEX);
            List<Map<String, String>> res = parseRaw(infoResponse);
            return res.stream()
                    .filter(m -> m.get(Keys.NS).equals(namespace))
                    .map(m -> m.get(Keys.INDEXNAME)).collect(Collectors.toList());
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

    /**
     * Cast an Id to its on-disk storage type
     *
     * @param origId raw id
     * @return id cast to on-disk type
     */
    public static Object idToStorageType(Object origId) {
        if (FireflyElement.class.isAssignableFrom(origId.getClass()))
            origId = ((FireflyElement) origId).id();
        if (Integer.class.equals(origId.getClass()))
            return ((Integer) origId).longValue();
        if (String.class.equals(origId.getClass()))
            return Long.parseLong((String) origId);
        return origId;
    }

    /**
     * return the set name for an elements properties
     *
     * @param elementClass class of element
     * @return name of Aerospike set
     */
    public String getElementPropertySet(final Class<? extends FireflyElement> elementClass) {
        if (FireflyEdge.class.isAssignableFrom(elementClass))
            return EDGE_AERO_SET;
        else if (FireflyVertex.class.isAssignableFrom(elementClass))
            return VERTEX_AERO_SET;
        else if (FireflyVertexProperty.class.isAssignableFrom(elementClass))
            return VERTEX_PROPERTY_AERO_SET;
        throw new UnsupportedOperationException("Element not supported " + elementClass.getName());
    }

    /**
     * Return the numeric id of the on-disk type
     *
     * @param clazz class to lookup
     * @return index of supported type
     */
    public Long getSupportedType(final Class clazz) {
        if (!SupportedValueTypes.containsKey(clazz))
            throw new UnsupportedOperationException(clazz.getName() + " is not a supported value type");
        return SupportedValueTypes.get(clazz);
    }

    /**
     * Create Indexes for Firefly
     */
    public void createGraphIndexes() {
        LOG.info("Creating graph indices.");
        List<String> existingIndexes = InfoOps.listExistingIndexes(getClient(), getNamespace());
        if (SUPERNODE_INDEX_ENABLED) {
            createIndex(existingIndexes, getElementPropertySet(FireflyEdge.class),
                    E_IN_INDEX, Direction.IN.name(),
                    IndexType.NUMERIC, IndexCollectionType.DEFAULT);
            createIndex(existingIndexes, getElementPropertySet(FireflyEdge.class),
                    E_OUT_INDEX, Direction.OUT.name(),
                    IndexType.NUMERIC, IndexCollectionType.DEFAULT);
        }

        createIndex(existingIndexes, getElementPropertySet(FireflyVertex.class),
                V_LABEL_INDEX, LABEL, IndexType.STRING, IndexCollectionType.DEFAULT);
        createIndex(existingIndexes, getElementPropertySet(FireflyEdge.class),
                E_LABEL_INDEX, LABEL, IndexType.STRING, IndexCollectionType.DEFAULT);

        createIndex(existingIndexes, getElementPropertySet(FireflyVertexProperty.class),
                STRING_VP_KV_INDEX,
                KEY_VALUE, IndexType.STRING, IndexCollectionType.MAPVALUES);
        createIndex(existingIndexes, getElementPropertySet(FireflyVertexProperty.class),
                NUMERIC_VP_KV_INDEX,
                KEY_VALUE, IndexType.NUMERIC, IndexCollectionType.MAPVALUES);

        createIndex(existingIndexes, getElementPropertySet(FireflyVertex.class),
                STRING_V_VP_KV_INDEX,
                VERTEX_PROPERTY_NAME_TO_VALUE, IndexType.STRING, IndexCollectionType.MAPVALUES);
        createIndex(existingIndexes, getElementPropertySet(FireflyVertex.class),
                NUMERIC_V_VP_KV_INDEX,
                VERTEX_PROPERTY_NAME_TO_VALUE, IndexType.NUMERIC, IndexCollectionType.MAPVALUES);

        createIndex(existingIndexes, getElementPropertySet(FireflyEdge.class),
                STRING_E_KV_INDEX,
                getElementPropertySet(FireflyEdge.class), IndexType.STRING, IndexCollectionType.MAPVALUES);
        createIndex(existingIndexes, getElementPropertySet(FireflyEdge.class),
                NUMERIC_E_KV_INDEX,
                getElementPropertySet(FireflyEdge.class), IndexType.NUMERIC, IndexCollectionType.MAPVALUES);
    }

    /**
     * Drop indices for Firefly
     */
    public void dropGraphIndices() {
        LOG.info("Dropping graph indices.");
        dropIndex(getElementPropertySet(FireflyVertex.class), LABEL);
        dropIndex(getElementPropertySet(FireflyEdge.class), LABEL);
        dropIndex(getElementPropertySet(FireflyVertex.class), V_LABEL_INDEX);
        dropIndex(getElementPropertySet(FireflyEdge.class), E_LABEL_INDEX);
        dropIndex(getElementPropertySet(FireflyVertexProperty.class), STRING_VP_KV_INDEX);
        dropIndex(getElementPropertySet(FireflyVertexProperty.class), NUMERIC_VP_KV_INDEX);
        dropIndex(getElementPropertySet(FireflyEdge.class), STRING_E_KV_INDEX);
        dropIndex(getElementPropertySet(FireflyEdge.class), NUMERIC_E_KV_INDEX);
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
     *
     * @param key Aerospike Key to read
     * @return Aerospike Record
     */
    protected Record read(final Key key) {
        readMetric.incrementAndGet();
        if (currentTraversal.get() != null && isCachedTraversal(currentTraversal.get()) && idFromTraversal(currentTraversal.get()).isPresent()) {
            try {
                Traversal.Admin traversal = currentTraversal.get();
                Optional<UUID> oid = idFromTraversal(traversal);
                UUID id = oid.get();
                TraversalCache c = traversalCacheSet.get(id);
                return c.read(key);
            } catch (Exception e) {
                LOG.debug(e.getMessage());
                return client.get(null, key);
            }
        } else {
            return client.get(null, key);
        }
    }

    /**
     * Perform a batch Aerospike read for a group of keys
     *
     * @param keys Array of Key to return records for
     * @return Array of Record
     */
    protected Record[] read(final Key[] keys) {
        this.readMetric.addAndGet(keys.length);
        return client.get(null, keys);
    }

    /**
     * Write to Aerospike, notify the cache implementation
     *
     * @param key  Key to write Bins into
     * @param bins Data Bin(s) to write
     */
    protected void write(final Key key, final Bin... bins) {
        writeMetric.incrementAndGet();
        if (currentTraversal.get() != null && idFromTraversal(currentTraversal.get()).isPresent()) {
            try {
                traversalCacheSet.get(idFromTraversal(currentTraversal.get()).get()).write(key, bins);
            } catch (Exception e) {
                LOG.debug(e.getMessage());
                client.put(sendKeyWritePolicy, key, bins);
            }
        } else {
            client.put(sendKeyWritePolicy, key, bins);
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
     * Delete by Key
     *
     * @param key Aerospike Key to delete
     */
    public void delete(final Key key) {
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

    /**
     * Issue a query on an index providing a custom filter
     *
     * @param setName   Name of Aerospike set
     * @param indexName Name of Index to query
     * @param filter    Custom Filter
     * @return Iterator of KeyRecord pair results
     */
    public Iterator<KeyRecord> queryIndex(String setName, String indexName, Filter filter) {
        final Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setName);
        stmt.setIndexName(indexName);
        stmt.setFilter(filter);
        final QueryPolicy p = new QueryPolicy();
        try {
            return client.query(p, stmt).iterator();
        } catch (AerospikeException ae) {
            throw new RuntimeException(ae);
        }
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
                                            final String mapKey) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, fid);
        if (fireflyRecord == null || fireflyRecord.record == null ||
                fireflyRecord.record.getMap(mapName) == null ||
                !fireflyRecord.record.getMap(mapName).containsKey(mapKey))
            return null;
        final Optional<? extends Map<?, ?>> map = Optional.ofNullable(fireflyRecord.record.getMap(mapName));
        if (!map.isPresent())
            return null;
        final Object val = map.get().get(mapKey);
        final Long typeHint = (Long) fireflyRecord.record.getMap(TYPE_HINTS).get(mapKey);
        if (val == null)
            return null;
        final Class clazz = SupportedTypeValues.get(typeHint);
        return (V) typeCast(clazz, val);
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
                                                                          final String mapName) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, fid);
        if (fireflyRecord == null || fireflyRecord.record == null || fireflyRecord.record.getMap(mapName).size() == 0)
            return null;
        final Optional<? extends Map<?, ?>> map = Optional.ofNullable(fireflyRecord.record.getMap(mapName));
        if (!map.isPresent())
            return null;
        final String mapKey = (String) map.get().keySet().iterator().next();
        final Long typeHint = (Long) fireflyRecord.record.getMap(TYPE_HINTS).get(mapKey);
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
                                             final String mapKey) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, fid);
        if (fireflyRecord == null)
            return;
        final Record r = fireflyRecord.record;
        final Map<String, Object> data;
        final Map<String, Object> typeHints;
        data = (Map<String, Object>) Optional.ofNullable(r.getMap(mapName)).orElse(new HashMap<>());
        typeHints = (Map<String, Object>) Optional.ofNullable(r.getMap(TYPE_HINTS)).orElse(new HashMap<>());

        if (!data.containsKey(mapKey)) {
            return;
        } else {
            data.remove(mapKey);
            typeHints.remove(mapKey);
        }
        final Bin typeHintBin = new Bin(TYPE_HINTS, Value.get(typeHints));
        final Bin valueBin = new Bin(mapName, Value.get(data));
        FireflyRecord.write(this, aeroSet, fid, valueBin, typeHintBin);
    }

    /**
     * Write a key-value pair into a Map on a Record.
     * Also store a type hint so it can be reconstructed as the correct type
     *
     * @param aeroSet
     * @param fid
     * @param mapName
     * @param mapKey
     * @param value
     * @param <V>
     */
    private <V> void writeTypeHintedValueToMap(final String aeroSet,
                                               final FireflyId fid,
                                               final String mapName,
                                               final String mapKey,
                                               final V value) {
        writeTypeHintedValueToMap(aeroSet, fid, mapName, mapKey, value, null);
    }

    /**
     * Write a key-value pair into a Map on a Record.
     * Also store a type hint so it can be reconstructed as the correct type
     * Pass additional Bins so 1 write can be made
     *
     * @param aeroSet
     * @param fid
     * @param mapName
     * @param mapKey
     * @param value
     * @param additionalBins
     * @param <V>
     */
    public <V> void writeTypeHintedValueToMap(final String aeroSet,
                                              final FireflyId fid,
                                              final String mapName,
                                              final String mapKey,
                                              final V value, Bin... additionalBins) {
        final Map<String, Object> data;
        final Map<String, Object> typeHints;
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, fid);
        if (fireflyRecord == null) {
            data = new HashMap<>();
            typeHints = new HashMap<>();
        } else {
            data = (Map<String, Object>) Optional.ofNullable(fireflyRecord.record.getMap(mapName)).orElse(new HashMap<>());
            typeHints = (Map<String, Object>) Optional.ofNullable(fireflyRecord.record.getMap(TYPE_HINTS)).orElse(new HashMap<>());
        }
        if (value != null)
            typeHints.put(mapKey, getSupportedType(value.getClass()));
        else
            typeHints.put(mapKey, null);
        data.put(mapKey, value);
        final Bin typeHintBin = new Bin(TYPE_HINTS, Value.get(typeHints));
        final Bin valueBin = new Bin(mapName, Value.get(data));
        if (additionalBins == null) {
            if (aeroSet.equals(EDGE_AERO_SET) || aeroSet.equals(VERTEX_AERO_SET) || aeroSet.equals(VERTEX_PROPERTY_AERO_SET)) {
                FireflyRecord.writeElement(this, aeroSet, fid, valueBin, typeHintBin);
            } else {
                FireflyRecord.write(this, aeroSet, fid, valueBin, typeHintBin);
            }
        } else {
            final List<Bin> listOfBins = Arrays.stream(additionalBins).collect(Collectors.toList());
            listOfBins.add(valueBin);
            listOfBins.add(typeHintBin);
            if (aeroSet.equals(EDGE_AERO_SET) || aeroSet.equals(VERTEX_AERO_SET) || aeroSet.equals(VERTEX_PROPERTY_AERO_SET)) {
                FireflyRecord.writeElement(this, aeroSet, fid, listOfBins.toArray(new Bin[0]));
            } else {
                FireflyRecord.write(this, aeroSet, fid, listOfBins.toArray(new Bin[0]));
            }
        }

    }

    /**
     * Cast a on-disk storage type to its user type
     *
     * @param clazz
     * @param val
     * @return
     */
    private Object typeCast(final Class clazz, final Object val) {
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
        final Record record = read(new Key(namespace, ID_MANAGER_SET, name));
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
        final Record record = client.operate(null, key,
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
     * @param name name of Counter to operate on
     * @return value of counter after operation
     */
    public long decrementIdCounter(final String name) {
        return decrementIdCounter(name, 1L);
    }

    /**
     * Decrement an Id counter.
     *
     * This is primarily used to reserve a range of Ids for use and management of reserved Ids must be handled explicitly.
     *
     * @param name name of Counter to operate on
     * @param amount amount on Counter to decrement
     * @return value of counter after operation
     */
    public long decrementIdCounter(final String name, final long amount) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, -amount);
        final Record record = client.operate(null, key,
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
        FireflyRecord.write(this, ID_MANAGER_SET, FireflyId.of(null, name), ctr);
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
        Record result = client.operate(null, key, ExpOperation.write(COUNTER, gtexp, ExpWriteFlags.DEFAULT), Operation.get(COUNTER));
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
        Record result = client.operate(null, key, ExpOperation.write(COUNTER, gtexp, ExpWriteFlags.DEFAULT), Operation.get(COUNTER));
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
        client.truncate(null, namespace, EDGE_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_PROPERTY_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, ID_MANAGER_SET, Calendar.getInstance());
        client.truncate(null, namespace, USER_SUPPLIED_ID_CACHE_SET, Calendar.getInstance());
        client.truncate(null, namespace, TEST_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_EDGELIST_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, GRAPH_VARIABLES_SET, Calendar.getInstance());
        client.truncate(null, namespace, GRAPH_METADATA_SET, Calendar.getInstance());
        client.truncate(null, namespace, INDEX_METADATA, Calendar.getInstance());
        client.truncate(null, namespace, OUT_VP_SET, Calendar.getInstance());
        client.truncate(null, namespace, IN_VP_SET, Calendar.getInstance());
        client.truncate(null, namespace, OUT_OUT_SET, Calendar.getInstance());
        client.truncate(null, namespace, OUT_IN_SET, Calendar.getInstance());
        client.truncate(null, namespace, IN_OUT_SET, Calendar.getInstance());
        client.truncate(null, namespace, IN_IN_SET, Calendar.getInstance());
        if (dropIndices)
            dropGraphIndices();
    }

    /**
     * Drop database by truncate, do not drop indices
     */
    public void dropDatabase() {
        dropDatabase(false);
    }

    /**
     * drop an Aerospike Index
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
     * create an Aerospike Index
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
            LOG.debug("Creating index {}:{}:{}.", set, indexName, binName);
        }
        final Policy policy = new Policy();
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            LOG.info("Will create index {}: {}", indexName, LocalDateTime.now());
            final IndexTask task = client.createIndex(policy, namespace, set, indexName, binName, type, indexCollectionType);
            task.waitTillComplete(1);
            LOG.debug("Completed create index {}: {}", indexName, LocalDateTime.now());
        } catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw new RuntimeException(ae);
            }
        }
    }

    /**
     * Create an Index on a particular Bin
     *
     * @param indexClass Firefly Element Class
     * @param binName    Name of Bin
     * @param idxType    Type of Index
     * @param idxColType Type of Index Collection
     * @param <T>        FireflyElement Type
     */
    public <T extends Element> void createBinIndex(Class<? extends FireflyElement> indexClass,
                                                   String binName,
                                                   IndexType idxType,
                                                   IndexCollectionType idxColType) {
        Key mKey = new Key(namespace, INDEX_METADATA, getElementPropertySet(indexClass));
        Record rec = read(mKey);

        List<String> keys = rec == null ? new ArrayList<String>() : (List<String>) rec.getList(INDEXED_BINS);
        keys.add(binName);
        Bin keysBin = new Bin(INDEXED_BINS, new ArrayList<>(new HashSet<>(keys)));
        client.put(null, mKey, keysBin);
        createIndex(new ArrayList<>(), getElementPropertySet(indexClass), binName, binName, idxType, idxColType);
    }

    /**
     * @param indexClass
     * @param key
     * @param <T>
     */
    public <T extends Element> void dropBinIndex(Class<? extends FireflyElement> indexClass, String key) {
        Key mKey = new Key(namespace, INDEX_METADATA, getElementPropertySet(indexClass));
        Record rec = read(mKey);
        List<String> keys = (List<String>) rec.getList(INDEXED_BINS);
        keys.remove(key);
        Bin keysBin = new Bin(INDEXED_BINS, new ArrayList<>(new HashSet<>(keys)));
        client.put(null, mKey, keysBin);
        dropIndex(getElementPropertySet(indexClass), key);
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
