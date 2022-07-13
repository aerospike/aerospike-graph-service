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
import com.aerospike.firefly.io.impl.standard.EdgeBackend;
import com.aerospike.firefly.io.impl.standard.VertexBackend;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.structure.util.FireflyHelper;
import com.aerospike.firefly.util.ConfigurationHelper;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class AerospikeConnection {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeConnection.class);
    public static final String ID_VALUE = "ID";
    private static final String NUMERIC_VP_KV_INDEX = "N_VP_KV";
    private static final String STRING_VP_KV_INDEX = "S_VP_KV";
    private static final String STRING_E_KV_INDEX = "S_E_KV";
    private static final String NUMERIC_E_KV_INDEX = "N_E_KV";
    private static final String INDEXED_BINS = "indexedBins";
    public static final String LABEL = "label";
    private static final String V_LABEL_INDEX = "v_label_idx";
    private static final String E_LABEL_INDEX = "e_label_idx";
    private static final String E_IN_INDEX = "v_label_idx";
    private static final String E_OUT_INDEX = "v_label_idx";
    private static final boolean SUPERNODE_INDEX_ENABLED = false;

    final int NumLoops = 2;
    final int CommandsPerEventLoop = 50;
    final int DelayQueueSize = 50;

    final EventLoops eventLoops;

    protected final String host;
    protected final int port;
    protected final AerospikeClient client;
    public final String namespace;

    public static final String IN_EDGES = "IN_EDGES";
    public static final String OUT_EDGES = "OUT_EDGES";
    public static final String CACHE_DISABLED = "CACHE_DISABLED";
    private static final String INDEX_METADATA = "INDEX_META";

    protected final String GRAPH_METADATA_SET;
    protected final String GRAPH_VARIABLES_SET;
    protected final String GRAPH_VARIABLES_RECORD;
    protected final String GRAPH_VARIABLES_MAP;
    public final String EDGE_AERO_SET;
    public final String VERTEX_AERO_SET;
    protected final String VERTEX_EDGELIST_AERO_SET;
    public final String VERTEX_PROPERTY_AERO_SET;
    protected final String EDGE_ID_KEY;
    protected final String EDGE_ID_BIN;
    protected final String VERTEX_ID_KEY;
    protected final String VERTEX_ID_BIN;
    protected final String VERTEX_PROPERTY_ID_KEY;
    protected final String VERTEX_PROPERTY_ID_BIN;
    public final String VERTEX_PROPERTY_NAME_TO_ID;
    public final String VERTEX_PROPERTY_NAME;
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
    private final Configuration conf;

    // User supplied id cache
    public final String USER_SUPPLIED_ID_CACHE_SET;
    public final String USER_SUPPLIED_ID_VERTEX_CACHE;
    public final String USER_SUPPLIED_ID_EDGE_CACHE;
    public final String USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE;

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

    private final int commandsPerLoop = 25;
    private final ClientPolicy clientPolicy;
    static AtomicLong readMetric = new AtomicLong(0);
    static AtomicLong writeMetric = new AtomicLong(0);


    public final Backend.Vertex vertexBackend;
    public final Backend.Edge edgeBackend;

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
    private String getElementPropertySet(final Class<? extends FireflyElement> elementClass) {
        if (elementClass.equals(FireflyEdge.class))
            return EDGE_AERO_SET;
        else if (elementClass.equals(FireflyVertex.class))
            return VERTEX_AERO_SET;
        else if (elementClass.equals(FireflyVertexProperty.class))
            return VERTEX_PROPERTY_AERO_SET;
        throw new UnsupportedOperationException("ele not supported " + elementClass.getClass());
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
     * Construct a new AerospikeConnection
     *
     * @param conf Apache Configuration
     */
    public AerospikeConnection(final Configuration conf) {
        LOG.info("Initializing AerospikeConnection.");

        this.conf = conf;
        final String host = conf.get(String.class, ConfigurationHelper.Keys.AEROSPIKE_HOST);
        final Integer port = conf.get(Integer.class, ConfigurationHelper.Keys.AEROSPIKE_PORT);
        final String namespace = conf.get(String.class, ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE);
        this.host = host;
        this.port = port;
        this.eventLoops = initializeEventLoops(EventLoopType.DIRECT_NIO, NumLoops, CommandsPerEventLoop, DelayQueueSize);
        final Host[] hosts = Host.parseHosts(host, port);
        this.clientPolicy = new ClientPolicy();
        this.clientPolicy.eventLoops = this.eventLoops;
        this.client = new AerospikeClient(clientPolicy, hosts);
        this.namespace = namespace;

        VERTEX_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.VERTEX_AERO_SET, conf);
        VERTEX_EDGELIST_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.VERTEX_EDGELIST_AERO_SET, conf);
        VERTEX_PROPERTY_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.VERTEX_PROPERTY_AERO_SET, conf);
        EDGE_ID_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_ID_KEY, conf);
        EDGE_ID_BIN = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_ID_BIN, conf);
        VERTEX_ID_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_ID_KEY, conf);
        VERTEX_ID_BIN = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_ID_BIN, conf);
        VERTEX_PROPERTY_ID_KEY = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_ID_KEY, conf);
        VERTEX_PROPERTY_ID_BIN = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_ID_BIN, conf);
        VERTEX_PROPERTY_NAME_TO_ID = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_NAME_TO_ID, conf);
        VERTEX_PROPERTY_NAME = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_NAME, conf);
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
        GRAPH_METADATA_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_METADATA_SET, conf);
        GRAPH_VARIABLES_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.Sets.GRAPH_VARIABLES_SET, conf);
        GRAPH_VARIABLES_RECORD = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_VARIABLES_RECORD, conf);
        GRAPH_VARIABLES_MAP = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_VARIABLES_MAP, conf);
        IN_EDGE_COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.IN_EDGE_COUNTER, conf);
        OUT_EDGE_COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.OUT_EDGE_COUNTER, conf);
        ID_CACHE_SIZE = Long.parseLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ID_CACHE_SIZE, conf));
        VP_COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VP_COUNTER, conf);

        // User supplied id cache.
        USER_SUPPLIED_ID_CACHE_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_CACHE_SET, conf);
        USER_SUPPLIED_ID_VERTEX_CACHE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_VERTEX_CACHE, conf);
        USER_SUPPLIED_ID_EDGE_CACHE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_EDGE_CACHE, conf);
        USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE, conf);

        vertexBackend = new VertexBackend(this);
        edgeBackend = new EdgeBackend(this);
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
     * Create Indexes for Firefly
     */
    public void createGraphIndexes() {
        final String graphName = this.conf.get(String.class, ConfigurationHelper.Keys.GRAPH_ID);
        LOG.info("Creating graph indices.");
        if (SUPERNODE_INDEX_ENABLED) {
            createIndex(getElementPropertySet(FireflyEdge.class),
                    E_IN_INDEX, graphName + "_" + Direction.IN.name(),
                    IndexType.NUMERIC, IndexCollectionType.DEFAULT);
            createIndex(getElementPropertySet(FireflyEdge.class),
                    E_OUT_INDEX, graphName + "_" + Direction.OUT.name(),
                    IndexType.NUMERIC, IndexCollectionType.DEFAULT);
        }

        createIndex(getElementPropertySet(FireflyVertex.class),
                graphName + "_" + V_LABEL_INDEX, LABEL, IndexType.STRING, IndexCollectionType.DEFAULT);
        createIndex(getElementPropertySet(FireflyEdge.class),
                graphName + "_" + E_LABEL_INDEX, LABEL, IndexType.STRING, IndexCollectionType.DEFAULT);

        createIndex(getElementPropertySet(FireflyVertexProperty.class),
                graphName + "_" + STRING_VP_KV_INDEX,
                KEY_VALUE, IndexType.STRING, IndexCollectionType.MAPVALUES);
        createIndex(getElementPropertySet(FireflyVertexProperty.class),
                graphName + "_" + NUMERIC_VP_KV_INDEX,
                KEY_VALUE, IndexType.NUMERIC, IndexCollectionType.MAPVALUES);
        createIndex(getElementPropertySet(FireflyEdge.class),
                graphName + "_" + STRING_E_KV_INDEX,
                getElementPropertySet(FireflyEdge.class), IndexType.STRING, IndexCollectionType.MAPVALUES);
        createIndex(getElementPropertySet(FireflyEdge.class),
                graphName + "_" + NUMERIC_E_KV_INDEX,
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

    public AerospikeClient getClient() {
        return this.client;
    }

    /**
     * Initialize the Aerospike Throttles
     *
     * @param numLoops
     * @param commandsPerEventLoop
     * @return
     */
    Throttles initializeThrottles(final int numLoops, final int commandsPerEventLoop) {
        final Throttles throttles = new Throttles(numLoops, commandsPerEventLoop);
        return throttles;
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
        this.readMetric.incrementAndGet();
        return client.get(null, key);
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
     * Determine if a vertexProperty exists
     *
     * @param vpId VertexProperty id to check
     * @return Boolean vertex property exists
     */
    public boolean vertexPropertyExists(final FireflyId vpId) {
        LOG.debug("Checking if vertex property {} exists.", vpId.value());
        final Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, vpId.toNumericId());
        return exists(key);
    }

    /**
     * query if the connected Aerospike instance is licenced for Enterprise Edition
     *
     * @return connected Aerospike instance is Enterprise Edition
     */
    public boolean aerospikeEnterprise() {
        return true; //@todo
    }

    /*
    @todo multi node test
    Joe Martin
      Keep in mind the replication Factor. You may need to divide by that
    */
    public long getSetSize(final String setName) {
        try {
            String infoQuery = "sets/" + namespace + "/" + setName;
            String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], infoQuery);
            List<Long> result = Arrays.stream(infoResponse.split(":"))
                    .filter(str -> str.startsWith("objects"))
                    .map(str -> Long.valueOf(str.split("=")[1]))
                    .collect(Collectors.toList());
            if (result.isEmpty()) {
                return 0;
            } else {
                return result.get(0);
            }
        } catch (AerospikeException ignored) {
            return 0;
        }
    }


    /**
     * Get a "fast count" of the number of elements in the Edge set using Aerospike info
     *
     * @return number of Edges
     */
    public long getEdgeCount() {
        return getSetSize(EDGE_AERO_SET);
    }

    private Iterator<KeyRecord> queryIndex(String setName, String indexName, Filter filter) {
        final Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setName);
        stmt.setIndexName(indexName);
        stmt.setFilter(filter);
        final QueryPolicy p = new QueryPolicy();
        return client.query(p, stmt).iterator();
    }

    /**
     * Lookup Edges with a particular property value by index
     *
     * @param graph FireflyGraph
     * @param key   Property Key
     * @param value Property Value being searched for
     * @return an Iterator of Edges
     */
    public Iterator<FireflyEdge> queryEdgePropertyStringMatchIndex(FireflyGraph graph, String key, Object value) {
        if (!String.class.isAssignableFrom(value.getClass())) {
            throw new RuntimeException(String.format("%s not a string", value.getClass()));
        }
        final Iterator<KeyRecord> rsi = queryIndex(EDGE_AERO_SET, STRING_E_KV_INDEX, Filter.contains(getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (String) value));
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr -> edgeBackend.edgeFromRecord(graph, kr.key, kr.record));
        return IteratorUtils.filter(edges, edge -> edge.property(key).value().equals(value));
    }

    public Iterator<FireflyEdge> queryEdgePropertyNumberMatchIndex(FireflyGraph graph, String key, P<?> predicate) {
        final Object value = predicate.getValue();
        Filter filter;
        if (Number.class.isAssignableFrom(value.getClass())) {
            if (Integer.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }

        final Iterator<KeyRecord> rsi = queryIndex(EDGE_AERO_SET, NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr -> edgeBackend.edgeFromRecord(graph, kr.key, kr.record));
        return IteratorUtils.filter(edges, edge -> edge.properties(key).hasNext());
    }

    public Iterator<FireflyEdge> queryEdgePropertyNumberRangeIndex(FireflyGraph graph, String key, P<?> predicate) {
        Filter filter;
        if (Number.class.isAssignableFrom(predicate.getValue().getClass())) {
            final long val = Long.class.isAssignableFrom(predicate.getValue().getClass()) ?
                    (long) predicate.getValue() : Long.valueOf((Integer) predicate.getValue());
            if (predicate.getBiPredicate().equals(Compare.lt))
                filter = Filter.range(getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }
        final Iterator<KeyRecord> rsi = queryIndex(EDGE_AERO_SET, NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr -> edgeBackend.edgeFromRecord(graph, kr.key, kr.record));
        return IteratorUtils.filter(edges, edge -> edge.properties(key).hasNext());
    }


    public Iterator<? extends Vertex> queryVertexLabelStringIndex(FireflyGraph graph, Object value) {
        final Iterator<KeyRecord> iter = queryIndex(VERTEX_AERO_SET, V_LABEL_INDEX, Filter.contains(LABEL, IndexCollectionType.DEFAULT, (String) value));
        final AerospikeConnection db = this;
        return IteratorUtils.map(iter, kr ->
                vertexBackend.vertexFromRecord(graph, FireflyRecord.fromRecord(db, kr.key, kr.record)));
    }

    public Iterator<? extends Edge> queryEdgeLabelStringIndex(FireflyGraph graph, Object value) {
        final Iterator<KeyRecord> iter = queryIndex(getElementPropertySet(FireflyEdge.class), E_LABEL_INDEX, Filter.contains(LABEL, IndexCollectionType.DEFAULT, (String) value));
        return IteratorUtils.map(iter, kr ->
                edgeBackend.edgeFromRecord(graph, kr.key, kr.record));
    }

    /**
     * Lookup VertexProperties with a particular Value by index
     *
     * @param graph FireflyGraph
     * @param key   Property Key
     * @param value Property Value being searched for
     * @return Iterator of VertexProperties
     */
    public Iterator<FireflyVertexProperty> queryVertexPropertyStringIndex(FireflyGraph graph, String key, Object value) {
        final Iterator<KeyRecord> rsi = queryIndex(VERTEX_PROPERTY_AERO_SET, STRING_VP_KV_INDEX, Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (String) value));
        final AerospikeConnection db = this;
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                vertexPropertyFromRecord(graph, FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyId.of(FireflyVertex.class, kr.record.getLong(PARENT_VERTEX_ID))));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }

    public Iterator<FireflyVertexProperty> queryVertexPropertyNumberMatchIndex(FireflyGraph graph, String key, P<?> predicate) {
        final Object value = predicate.getValue();
        Filter filter;
        if (Number.class.isAssignableFrom(value.getClass())) {
            if (Integer.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }
        final Iterator<KeyRecord> rsi = queryIndex(VERTEX_PROPERTY_AERO_SET, NUMERIC_E_KV_INDEX, filter);
        final AerospikeConnection db = this;

        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                vertexPropertyFromRecord(graph, FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyId.of(FireflyVertex.class, kr.record.getLong(PARENT_VERTEX_ID))));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }

    public Iterator<FireflyVertexProperty> queryVertexPropertyNumberRangeIndex(FireflyGraph graph, String key, P<?> predicate) {
        Filter filter;
        if (Number.class.isAssignableFrom(predicate.getValue().getClass())) {
            final long val = Long.class.isAssignableFrom(predicate.getValue().getClass()) ?
                    (long) predicate.getValue() : Long.valueOf((Integer) predicate.getValue());
            if (predicate.getBiPredicate().equals(Compare.lt))
                filter = Filter.range(KEY_VALUE, IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(KEY_VALUE, IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }

        Iterator<KeyRecord> rsi = queryIndex(VERTEX_PROPERTY_AERO_SET, NUMERIC_VP_KV_INDEX, filter);
        final AerospikeConnection db = this;
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                (FireflyVertexProperty) vertexPropertyFromRecord(graph, FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyId.of(FireflyVertex.class, kr.record.getLong(PARENT_VERTEX_ID))));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }

    public long getWriteMetric() {
        return writeMetric.get();
    }

    public long getReadMetric() {
        return readMetric.get();
    }


    /**
     * manage the set names for an element type
     */
    static class id_config {
        private final Class<? extends FireflyElement> type;
        private final String AERO_SET;
        private final String ID_KEY;
        private final String ID_BIN;

        public id_config(AerospikeConnection ac, final Class<? extends FireflyElement> type) {
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

        String getAeroSet() {
            return AERO_SET;
        }

        String getIdBin() {
            return ID_BIN;
        }

    }

    /**
     * Return an iterator of all the (raw) ids in a set
     *
     * @param setName name of Aerospike set to scan
     * @return an Iterator of raw Long id values
     */
    protected Iterator<Long> scanAllIdsInSet(final String setName) {
        //@todo performance
        LOG.trace("Scanning {} ids.", setName);
        final Iterator<Map.Entry<Key, Record>> i = scanAllKeysInSet(setName, null);
        return IteratorUtils.map(i, keyRecordEntry -> NumericIdManager.convert(keyRecordEntry.getKey().userKey.getObject()));
    }

    /**
     * Scan a set for keys matched by the provided Expression. convert them to their raw id.
     *
     * @param setName Aerospike set to scan
     * @param exp     Aerospike filter Expression to apply to Scan
     * @return Iterator of raw Object ids
     */
    public Iterator<Object> scanFilteredIdsInSet(final String setName, final Expression exp) {
        LOG.trace("Scanning {} ids with filter {}.", setName, exp);
        final Iterator<Map.Entry<Key, Record>> i = scanAllKeysInSet(setName, exp);
        return IteratorUtils.map(i, keyRecordEntry -> {
            return keyRecordEntry.getKey().userKey.getObject();
        });
    }

    /**
     * Issue a Scan query to for all the Keys in a set
     *
     * @param setName  Aerospike set to scan
     * @param exp      Aerospike filter Expression to apply to scan
     * @param binNames array of Bin names to read into Records returned
     * @return Iterator of Map.Entry Key, Record matched by Scan query
     */
    protected Iterator<Map.Entry<Key, Record>> scanAllKeysInSet(final String setName, final Expression exp, String... binNames) {
        LOG.trace("Scanning all ids in {}:{} with filter {}.", setName, Arrays.toString(binNames), exp);
        ScanPolicy policy = new ScanPolicy();
        policy.includeBinData = false;
        return scanAllRecordsInSet(setName, exp, policy, binNames);
    }

    /**
     * Issue a scan query for all the records in a set.
     *
     * @param setName Aerospike set name to scan
     * @return Iterator of Map.Entry Key, Record
     */
    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName) {
        LOG.trace("Scanning all records in {}.", setName);
        return scanAllRecordsInSet(setName, null);
    }

    /**
     * Issue a scan query for all the records in a set.
     * Filter by an Exp, optionally provide binNames to return
     *
     * @param setName  Aerospike set name to scan
     * @param exp      Expression to apply to Scan
     * @param binNames Bin names to read into Records returned by Scan
     * @return Iterator of Map.Entry Key, Record
     */
    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName, final Expression exp, String... binNames) {
        LOG.trace("Scanning all records in {}:{} with filter {}.", setName, Arrays.toString(binNames), exp);
        return scanAllRecordsInSet(setName, exp, new ScanPolicy(), binNames);
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
    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName, final Expression exp, ScanPolicy policy, String... binNames) {
        LOG.trace("Issuing scan query of all records in {}:{}:{} with filter {}.", namespace, setName, Arrays.toString(binNames), exp);
        final Throttles throttles = initializeThrottles(this.eventLoops.getSize(), this.commandsPerLoop);
        final Monitor scanMonitor = new Monitor();
        final int progressFreq = 100;
        policy.sendKey = true;
        if (exp != null)
            policy.filterExp = exp;
        final ScanRecordSequenceListener listener = new ScanRecordSequenceListener(eventLoops,
                throttles,
                scanMonitor,
                client,
                progressFreq);
        client.scanAll(this.eventLoops.next(), listener, policy, this.namespace, setName, binNames);
        //@todo performance
        // should return custom iterator that produces results while query is running
        // custom iterator .hasNext() should return false once query is complete
        scanMonitor.waitTillComplete();

        return listener.iterator();
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
    private <V> V readTypeHintedValueFromMap(final String aeroSet,
                                             final FireflyId fid,
                                             final String mapName,
                                             final String mapKey) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, fid);
        if (fireflyRecord == null || !fireflyRecord.record.getMap(mapName).containsKey(mapKey))
            throw new NoSuchElementException();
        final Optional<? extends Map<?, ?>> map = Optional.ofNullable(fireflyRecord.record.getMap(mapName));
        if (!map.isPresent())
            return null;
        final Object val = map.get().get(mapKey);
        final Long typeHint = (Long) fireflyRecord.record.getMap(TYPE_HINTS).get(mapKey);
        if (val == null)
            return null;
        final Class clazz = SupportedValueTypes.entrySet()
                .stream()
                .filter(entry -> typeHint.equals(entry.getValue()))
                .map(Map.Entry::getKey).collect(Collectors.toList()).get(0);
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
    private <V> AbstractMap.Entry<String, V> readTypeHintedKeyValueFromMap(final String aeroSet,
                                                                           final FireflyId fid,
                                                                           final String mapName) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, fid);
        if (fireflyRecord == null || fireflyRecord.record.getMap(mapName).size() == 0)
            throw new NoSuchElementException();
        final Optional<? extends Map<?, ?>> map = Optional.ofNullable(fireflyRecord.record.getMap(mapName));
        if (!map.isPresent())
            return null;
        final String mapKey = (String) map.get().keySet().iterator().next();
        final Long typeHint = (Long) fireflyRecord.record.getMap(TYPE_HINTS).get(mapKey);
        final Object val = map.get().values().iterator().next();

        if (val == null)
            return null;
        final Class clazz = SupportedValueTypes.entrySet()
                .stream()
                .filter(entry -> typeHint.equals(entry.getValue()))
                .map(Map.Entry::getKey).collect(Collectors.toList()).get(0);
        return new AbstractMap.SimpleEntry<>(mapKey, (V) typeCast(clazz, val));
    }

    /**
     * remove a key-value from a map along with its type-hint
     *
     * @param aeroSet
     * @param fid
     * @param mapName
     * @param mapKey
     */
    private void removeTypeHintedValueFromMap(final String aeroSet,
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
            List<Bin> listOfBins = Arrays.stream(additionalBins).collect(Collectors.toList());
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
     * Return a Graph variable value by name
     *
     * @param key Graph variable key
     * @param <V> type
     * @return Graph variable value
     */
    public <V> V readGraphVariable(final String key) {
        return readTypeHintedValueFromMap(GRAPH_VARIABLES_SET, FireflyId.of(null, GRAPH_VARIABLES_RECORD), GRAPH_VARIABLES_MAP, key);
    }

    /**
     * Return a Set of all the Graph variable names
     *
     * @return Set of Graph variable keys
     */
    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, GRAPH_VARIABLES_SET, FireflyId.of(null, GRAPH_VARIABLES_RECORD));
        if (fireflyRecord == null)
            return new HashSet<>();
        final Map<String, ?> m = (Map<String, ?>) fireflyRecord.record.getMap(GRAPH_VARIABLES_MAP);
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
        writeTypeHintedValueToMap(GRAPH_VARIABLES_SET, FireflyId.of(null, GRAPH_VARIABLES_RECORD), GRAPH_VARIABLES_MAP, key, value);
    }

    /**
     * Remove a Graph variable
     *
     * @param key Graph variable key to remove
     * @param <V> Graph variable type
     */
    public <V> void removeGraphVariable(final String key) {
        removeTypeHintedValueFromMap(GRAPH_VARIABLES_SET, FireflyId.of(null, GRAPH_VARIABLES_RECORD), GRAPH_VARIABLES_MAP, key);
    }


    /**
     * Read a single VertexProperty from its id
     *
     * @param parent Vertex that owns the VertexProperty being looked up
     * @param vpId   Id of VertexProperty to lookup
     * @param <V>    type
     * @return VertexProperty
     */
    public <V> FireflyVertexProperty<V> readVertexProperty(final FireflyVertex parent, final FireflyId vpId) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, VERTEX_PROPERTY_AERO_SET, vpId);
        if (fireflyRecord == null)
            throw new NoSuchElementException();
        return vertexPropertyFromRecord((FireflyGraph) parent.graph(), fireflyRecord, parent.id);
    }

    /**
     * Construct a VertexProperty object from a record
     *
     * @param fireflyRecord FireflyRecord with VertexProperty data
     * @param parentId      parent Vertex Id
     * @param <V>           type
     * @return FireflyVertexProperty
     */
    public <V> FireflyVertexProperty<V> vertexPropertyFromRecord(FireflyGraph graph, FireflyRecord fireflyRecord, final FireflyId parentId) {
        FireflyId fid = FireflyId.of(FireflyVertexProperty.class, fireflyRecord.id());
        final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(readTypeHintedKeyValueFromMap(VERTEX_PROPERTY_AERO_SET, fid, KEY_VALUE));
        if (kv.isEmpty())
            return new FireflyVertexProperty<V>(graph, fid, parentId, null, null);
        final String vpKey = kv.get().getKey();
        final Object vpVal = kv.get().getValue();
        return new FireflyVertexProperty<V>(graph, fid, parentId, vpKey, (V) vpVal);
    }


    /**
     * Read the struct of vertex properties for an associated Vertex
     * the vertex record has a Map[String,List[ID]] inside it.
     * from this each VertexProperty is read from its own record by id
     *
     * @param vertex parent Vertex
     * @return Map of label to list of VertexProperty
     */
    public Map<String, List<VertexProperty>> readVertexPropertiesByScan(final FireflyVertex vertex) {
        final Object origId = vertex.id();
        final Long storageId = (Long) idToStorageType(origId);
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(PARENT_VERTEX_ID),
                        Exp.val(storageId))
        );
        final Iterator<Map.Entry<Key, Record>> records = scanAllRecordsInSet(VERTEX_PROPERTY_AERO_SET, exp);
        //all results, empty
        final Map<String, List<VertexProperty>> results = new HashMap<>();
        //for every vp id associated with vertex
        records.forEachRemaining(entry -> {
            //load the vp
            final VertexProperty<Object> vp = vertexPropertyFromRecord((FireflyGraph) vertex.graph(), FireflyRecord.fromRecord(this, entry.getKey(), entry.getValue()), vertex.id);
            //if there is a list for its key, get it, else, create it
            final List<VertexProperty> list = results.getOrDefault(vp.key(), new ArrayList<>());
            //add the vp to the list named for its key
            list.add(vp);
            //put the list back
            results.put(vp.key(), list);
        });
        return results;
    }

    /**
     * Return a single VertexProperty associated with a Vertex and key if vertex is in cache. Otherwise scan and return result.
     *
     * @param vertex parent Vertex
     * @return List of VertexProperty for provided key
     */
    public List<VertexProperty> readVertexProperty(final FireflyVertex vertex, final String key) {
        final FireflyRecord r = vertexBackend.getVertexRecord(vertex);
        if (r == null) {
            return new ArrayList<>();
        }

        if (r.record.getLong(VP_COUNTER) < ID_CACHE_SIZE) {
            final Map<String, List<Long>> idMap = (Map<String, List<Long>>) r.record.getMap(VERTEX_PROPERTY_NAME_TO_ID);
            if (idMap == null) {
                return new ArrayList<>();
            }

            final List<Long> vp = idMap.getOrDefault(key, null);
            if (vp == null) {
                return new ArrayList<>();
            }

            final List<VertexProperty> vpList = new ArrayList<>();
            for (Long id : vp) {
                FireflyId fireflyId = FireflyId.of(FireflyVertexProperty.class, id);
                final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(readTypeHintedKeyValueFromMap(VERTEX_PROPERTY_AERO_SET, fireflyId, KEY_VALUE));
                if (kv.isEmpty())
                    vpList.add(new FireflyVertexProperty((FireflyGraph) vertex.graph(), fireflyId, vertex.id, null, null));
                else
                    vpList.add(new FireflyVertexProperty((FireflyGraph) vertex.graph(), fireflyId, vertex.id, kv.get().getKey(), kv.get().getValue()));
            }
            return vpList;
        } else {
            return readVertexPropertiesByScan(vertex).get(key);
        }
    }

    /**
     * return a map of VertexProperties associated with a Vertex
     *
     * @param vertex parenet Vertex
     * @return Map of label to List of VertexProperty
     */
    public Map<String, List<VertexProperty>> readVertexProperties(final FireflyVertex vertex) {
        FireflyRecord r = vertexBackend.getVertexRecord(vertex);
        if (r == null) {
            return new HashMap<>();
        }
        long vp_count = r.record.getLong(VP_COUNTER);
        if (vp_count < ID_CACHE_SIZE) {
            final Map<String, List<Long>> idMap = this.vertexBackend.getXXXIdsFromVertexLabelMap(vertex, VERTEX_PROPERTY_NAME_TO_ID);
            final Map<String, List<VertexProperty>> vpLabelList = new HashMap<>();
            idMap.entrySet().forEach(entry -> {
                String label = entry.getKey();
                List<Long> idList = entry.getValue();
                List<VertexProperty> vpList = new ArrayList<>();
                idList.forEach(id -> {
                    vpList.add(readVertexProperty(vertex, FireflyId.of(FireflyVertexProperty.class, id)));
                });
                vpLabelList.put(label, vpList);
            });
            return vpLabelList;

        } else
            return readVertexPropertiesByScan(vertex);
    }


    /**
     * Write a new vertex property
     *
     * @param vertex parent Vertex
     * @param vpid   VertexProperty id to write
     * @param vpk    VP key
     * @param key    VP key
     * @param value  VP value
     * @param <V>    type
     */
    public <V> void writeVertexProperty(final FireflyVertex vertex,
                                        final FireflyId vpid,
                                        final String vpk,
                                        final String key,
                                        final V value) {
        final Bin vpkBin = new Bin(VERTEX_PROPERTY_NAME, vpk);
        final Bin pviBin = new Bin(PARENT_VERTEX_ID, idToStorageType(vertex.id()));
        writeTypeHintedValueToMap(VERTEX_PROPERTY_AERO_SET, vpid, KEY_VALUE, key, value, vpkBin, pviBin);
        addVPToVertex(vertex, readVertexProperty(vertex, vpid));
    }

    /**
     * @param vertex Vertex to operate on
     * @param vp     VertexProperty to remove from Vertex id cache
     */
    public void removeIdFromVertexPropertyList(final FireflyVertex vertex, final VertexProperty vp) {
        final FireflyRecord vertexRecord = vertexBackend.getVertexRecord(vertex);
        if (vertexRecord == null)
            throw new NoSuchElementException();
        Map<String, List<Object>> propertyKeys = (Map<String, List<Object>>) vertexRecord.record.getMap(VERTEX_PROPERTY_NAME_TO_ID);
        if (propertyKeys == null)
            propertyKeys = new HashMap<>();
        final List<Object> ids = propertyKeys.getOrDefault(vp.key(), new ArrayList<>());
        ids.remove(vp.id());
        if (ids.isEmpty())
            propertyKeys.remove(vp.key());
        else
            propertyKeys.put(vp.key(), ids);
        long vpCounter = vertexRecord.record().getLong(VP_COUNTER);
        if (vpCounter > 0)
            vpCounter--;
        if (vpCounter == ID_CACHE_SIZE - 1) // if id set size within cache size, restore the cache
            propertyKeys = readVertexPropertiesByScan(vertex).entrySet().stream().map(entry -> {
                return new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().stream().map(Element::id));
            }).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, it -> (List<Object>) it.getValue()));

        final Bin vpCounterBin = new Bin(VP_COUNTER, Value.get(vpCounter));
        final Bin vertexPropertyIds = new Bin(VERTEX_PROPERTY_NAME_TO_ID, Value.get(propertyKeys));
        FireflyRecord.writeElement(this, VERTEX_AERO_SET, vertex.id, vertexPropertyIds, vpCounterBin);
    }

    /**
     * Remove a VertexProperty Record from the database
     *
     * @param property VertexProperty to remove
     */
    public void removeVertexProperty(final FireflyVertexProperty property) {
        final Key key = FireflyRecord.getKey(namespace, VERTEX_PROPERTY_AERO_SET, FireflyId.fromElement(property));
        final Vertex parent = property.element();
        removeIdFromVertexPropertyList((FireflyVertex) parent, property);
        delete(key);
    }

    /**
     * Read the set of properties for an associated element
     * A record with the id of its element is read from the Aerospike set PROPERTY_AERO_SET
     *
     * @param element element to read properties from
     * @param <V>     type
     * @return Map of Label to Property
     */

    public <V> Map<String, Property> readProperties(final FireflyElement element) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, getElementPropertySet(element.getClass()), FireflyId.fromElement(element).toNumericId());
        if (fireflyRecord == null)
            return new HashMap<>();

        final Map<String, Property> result = new HashMap<>();
        final Map<String, V> data = (Map<String, V>) fireflyRecord.record.getMap(getElementPropertySet(element.getClass()));
        if (data == null)
            return result;
        data.forEach((key1, value) -> {
            Property<V> prop = readProperty(element, key1);
            result.put(key1, prop);
        });
        return result;
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * construct and return a Property from the value associated with k in the ELEMENT_PROPERTIES map
     *
     * @param element Element to read property from
     * @param key     property key
     * @param <V>     type
     * @return Property
     */
    public <V> Property readProperty(final FireflyElement element, final String key) {
        return new FireflyProperty(element, key, readTypeHintedValueFromMap(getElementPropertySet(element.getClass()), FireflyId.fromElement(element).toNumericId(), getElementPropertySet(element.getClass()), key));
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
            return Math.toIntExact((Long) val);
        return clazz.cast(val);
    }

    /**
     * write a new property into the property Record for element
     * 1 property record per element, a Map bin of name to value
     *
     * @param id    Element id to write to
     * @param clazz Element type
     * @param key   property key
     * @param value property value to write
     * @param <V>   type
     */
    public <V> void writeProperty(final FireflyId id, final Class<? extends FireflyElement> clazz, final String key, final V value) {
        FireflyHelper.validatePropertyValue(value);
        writeTypeHintedValueToMap(getElementPropertySet(clazz), id, getElementPropertySet(clazz), key, value);
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * remove k from the ELEMENT_PROPERTIES map
     *
     * @param element Element to remove property from
     * @param key     property key to remove
     * @param <V>     type
     */
    public <V> void removeProperty(final FireflyElement element, final String key) {
        removeTypeHintedValueFromMap(getElementPropertySet(element.getClass()), FireflyId.fromElement(element), getElementPropertySet(element.getClass()), key);
    }

    /**
     * get a list of currently valid ids
     *
     * @param type type of Element
     * @return Iterator of raw Ids
     */
    public Iterator<?> readElementIds(final Class<? extends FireflyElement> type) {
        final id_config cfg = new id_config(this, type);
        return scanAllIdsInSet(cfg.getAeroSet());
    }

    /**
     * Add a VertexProperty to a Vertex
     *
     * @param vertex
     * @param vp
     */
    private void addVPToVertex(FireflyVertex vertex, FireflyVertexProperty vp) {
        LOG.debug("Adding vertex property {} to vertex {}.", vp, vertex);
        final FireflyRecord fireflyRecord = vertexBackend.getVertexRecord(vertex);

        Map<String, List<Long>> labelIds;
        if (fireflyRecord == null || fireflyRecord.record() == null) {
            labelIds = new HashMap<>();
        } else {
            labelIds = (Map<String, List<Long>>) Optional.ofNullable(fireflyRecord.record().getMap(VERTEX_PROPERTY_NAME_TO_ID)).orElse(new HashMap<>());
        }

        long vpCounter = 0;
        if (fireflyRecord != null && fireflyRecord.record() != null) {
            vpCounter = fireflyRecord.record().getLong(VP_COUNTER);
        }

        final List<Long> ids = labelIds.getOrDefault(vp.key(), new ArrayList<>());
        if (vpCounter < ID_CACHE_SIZE)
            ids.add(NumericIdManager.convert(vp.id()));
        vpCounter++;

        labelIds.put(vp.key(), ids);
        final Bin edgeData = new Bin(VERTEX_PROPERTY_NAME_TO_ID, Value.get(labelIds));
        final Bin edgeCounterBin = new Bin(VP_COUNTER, Value.get(vpCounter));
        FireflyRecord.writeElement(this, VERTEX_AERO_SET, vertex.id, edgeData, edgeCounterBin);
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
     * decrement an Id counter
     *
     * @param name name of Counter to operate on
     * @return value of counter after operation
     */
    public long decrementIdCounter(final String name) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, -1);
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
     * if the offered value is greater then the current counter value
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
     * truncate all the sets associated with the Graph
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
        client.truncate(null, namespace, INDEX_METADATA, Calendar.getInstance());
        if (dropIndices)
            dropGraphIndices();
    }

    public void dropDatabase() {
        dropDatabase(false);
    }

    @Override
    public final String toString() {
        return String.format("aerospike://%s:%s/%s", host, port, namespace);
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
            task.waitTillComplete();
        } catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
                throw new RuntimeException(ae);
            }
        }
    }

    /**
     * create an Aerospike Index
     *
     * @param set                 Set name
     * @param indexName           Index name
     * @param binName             Bin name to be indexed
     * @param type                Index type
     * @param indexCollectionType Index Collection Type
     */
    public void createIndex(
            final String set,
            final String indexName,
            final String binName,
            final IndexType type,
            final IndexCollectionType indexCollectionType) {
        LOG.debug("Creating index {}:{}:{}.", set, indexName, binName);
        final Policy policy = new Policy();
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            final IndexTask task = client.createIndex(policy, namespace, set, indexName, binName, type, indexCollectionType);
            task.waitTillComplete();
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
        createIndex(getElementPropertySet(indexClass), binName, binName, idxType, idxColType);
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


    /**
     * close the connection to Aerospike
     */
    public void close() {
        LOG.info("Closing client.");
        this.client.close();

        LOG.info("Closing event loop.");
        this.eventLoops.close();
    }
}
