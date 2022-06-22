package com.aerospike.firefly.io;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.async.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import com.aerospike.firefly.util.ConfigurationHelper;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class AerospikeConnection {
    public static final String ID_VALUE = "ID";
    final int NumLoops = 2;
    final int CommandsPerEventLoop = 50;
    final int DelayQueueSize = 50;

    final EventLoops eventLoops;

    protected final String host;
    protected final int port;
    protected final AerospikeClient client;
    public final String namespace;

    private static final String IN_EDGES = "IN_EDGES";
    private static final String OUT_EDGES = "OUT_EDGES";
    private static final String CACHE_DISABLED = "CACHE_DISABLED";
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
    protected final String VERTEX_PROPERTY_NAME_TO_ID;
    protected final String VERTEX_PROPERTY_NAME;
    protected final String PARENT_VERTEX_ID;

    protected final String IN_EDGE_COUNTER;
    protected final String OUT_EDGE_COUNTER;
    protected final String VP_COUNTER;
    protected final long ID_CACHE_SIZE;
    protected final String EDGE_PROPERTIES;
    protected final String VP_PROPERTIES;
    protected final String TYPE_HINTS;
    protected final String KEY_VALUE;
    protected final String COUNTER;
    protected final String ID_MANAGER_SET;
    public final String ID_TYPE;
    public final String GLOBAL;
    public final String TEST_SET;
    private final Configuration conf;


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


    /**
     * Cast an Id to its on-disk storage type
     *
     * @param origId raw id
     * @return id cast to on-disk type
     */
    private static Object idToStorageType(Object origId) {
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
    private Long getSupportedType(final Class clazz) {
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

        VERTEX_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_AERO_SET, conf);
        VERTEX_EDGELIST_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_EDGELIST_AERO_SET, conf);
        VERTEX_PROPERTY_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_AERO_SET, conf);
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
        ID_MANAGER_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ID_MANAGER_SET, conf);
        ID_TYPE = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ID_TYPE, conf);
        GLOBAL = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GLOBAL, conf);
        TEST_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.TEST_SET, conf);
        EDGE_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_AERO_SET, conf);
        GRAPH_METADATA_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_METADATA_SET, conf);
        GRAPH_VARIABLES_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_VARIABLES_SET, conf);
        GRAPH_VARIABLES_RECORD = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_VARIABLES_RECORD, conf);
        GRAPH_VARIABLES_MAP = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.GRAPH_VARIABLES_MAP, conf);
        IN_EDGE_COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.IN_EDGE_COUNTER, conf);
        OUT_EDGE_COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.OUT_EDGE_COUNTER, conf);
        ID_CACHE_SIZE = Long.parseLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ID_CACHE_SIZE, conf));
        VP_COUNTER = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VP_COUNTER, conf);
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
                System.out.println("Error: Invalid event loop type");
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
        return client.get(null, key);
    }

    /**
     * Determine of a key exists
     *
     * @param key Aerospike Key to check
     * @return Boolean key exists
     */
    protected boolean exists(final Key key) {
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
     * Determine of a Vertex exists
     *
     * @param vertexId Id of Vertex to check
     * @return Boolean vertex exists
     */
    public boolean vertexExists(final FireflyId vertexId) {
        final Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, vertexId);
        return exists(key);
    }

    /**
     * Determine if an Edge exists
     *
     * @param edgeId edge id to check
     * @return Boolean edge exists
     */
    public boolean edgeExists(final FireflyId edgeId) {
        final Key key = FireflyRecord.getKey(namespace, EDGE_AERO_SET, edgeId);
        return exists(key);
    }

    /**
     * Determine if a vertexProperty exists
     *
     * @param vpId VertexProperty id to check
     * @return Boolean vertex property exists
     */
    public boolean vertexPropertyExists(final FireflyId vpId) {
        final Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, vpId);
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


    /**
     * manage the set names for an element type
     */
    public static class id_config {
        private final Class<? extends FireflyElement> type;
        private final String AERO_SET;
        private final String ID_KEY;
        private final String ID_BIN;

        id_config(AerospikeConnection ac, final Class<? extends FireflyElement> type) {
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
        final Iterator<Map.Entry<Key, Record>> i = scanAllKeysInSet(setName, null);
        return IteratorUtils.map(i, keyRecordEntry -> {
            return keyRecordEntry.getKey().userKey.toLong();
        });
    }

    /**
     * Scan a set for keys matched by the provided Expression. convert them to their raw id.
     *
     * @param setName Aerospike set to scan
     * @param exp     Aerospike filter Expression to apply to Scan
     * @return Iterator of raw Object ids
     */
    protected Iterator<Object> scanFilteredIdsInSet(final String setName, final Expression exp) {
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
        return scanAllRecordsInSet(setName, exp, new ScanPolicy(), binNames);
    }

    /**
     * Issue a scan query for all the records in a set.
     * Filter by an Exp, provide a ScanPolicy, optionally provide binNames to return
     *
     * @param setName  Aerospike set name to scan
     * @param exp      Expression to apply to Scan
     * @param policy   ScanPolicy to use during Scan
     * @param binNames Bin names to read into Records returned
     * @return Iterator of Map.Entry Key, Record
     */
    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName, final Expression exp, ScanPolicy policy, String... binNames) {
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
                                                                           final String mapName
    ) {
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
    private <V> void writeTypeHintedValueToMap(final String aeroSet,
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
            FireflyRecord.write(this, aeroSet, fid, valueBin, typeHintBin);
        } else {
            List<Bin> listOfBins = Arrays.stream(additionalBins).collect(Collectors.toList());
            listOfBins.add(valueBin);
            listOfBins.add(typeHintBin);
            FireflyRecord.write(this, aeroSet, fid, listOfBins.toArray(new Bin[0]));
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
        return readTypeHintedValueFromMap(GRAPH_VARIABLES_SET, FireflyId.of(this, null, GRAPH_VARIABLES_RECORD), GRAPH_VARIABLES_MAP, key);
    }

    /**
     * Return a Set of all the Graph variable names
     *
     * @return Set of Graph variable keys
     */
    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, GRAPH_VARIABLES_SET, FireflyId.of(this, null, GRAPH_VARIABLES_RECORD));
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
        writeTypeHintedValueToMap(GRAPH_VARIABLES_SET, FireflyId.of(this, null, GRAPH_VARIABLES_RECORD), GRAPH_VARIABLES_MAP, key, value);
    }

    /**
     * Remove a Graph variable
     *
     * @param key Graph variable key to remove
     * @param <V> Graph variable type
     */
    public <V> void removeGraphVariable(final String key) {
        removeTypeHintedValueFromMap(GRAPH_VARIABLES_SET, FireflyId.of(this, null, GRAPH_VARIABLES_RECORD), GRAPH_VARIABLES_MAP, key);
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
        return vertexPropertyFromRecord((FireflyGraph) parent.graph(), fireflyRecord, parent);
    }

    /**
     * Construct a VertexProperty object from a record
     *
     * @param fireflyRecord FireflyRecord with VertexProperty data
     * @param parent        parent Vertex
     * @param <V>           type
     * @return FireflyVertexProperty
     */
    public <V> FireflyVertexProperty<V> vertexPropertyFromRecord(FireflyGraph graph, FireflyRecord fireflyRecord, final FireflyVertex parent) {
        FireflyId fid = FireflyId.of(this, FireflyVertexProperty.class, fireflyRecord.id());
        final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(readTypeHintedKeyValueFromMap(VERTEX_PROPERTY_AERO_SET, fid, KEY_VALUE));
        if (kv.isEmpty())
            return new FireflyVertexProperty<V>(graph, fid, parent, null, null);
        final String vpKey = kv.get().getKey();
        final Object vpVal = kv.get().getValue();
        return new FireflyVertexProperty<V>(graph, fid, parent, vpKey, (V) vpVal);
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
            final VertexProperty<Object> vp = vertexPropertyFromRecord((FireflyGraph) vertex.graph(), FireflyRecord.fromRecord(this, entry.getKey(), entry.getValue()), vertex);
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
     * return a map of VertexProperties associated with a Vertex
     *
     * @param vertex parenet Vertex
     * @return Map of label to List of VertexProperty
     */
    public Map<String, List<VertexProperty>> readVertexProperties(final FireflyVertex vertex) {
        FireflyRecord r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex));
        long vp_count = r.record.getLong(VP_COUNTER);
        if (vp_count < ID_CACHE_SIZE) {
            final Map<String, List<Long>> idMap = getXXXIdsFromVertexLabelMap(vertex, VERTEX_PROPERTY_NAME_TO_ID);
            final Map<String, List<VertexProperty>> vpLabelList = new HashMap<>();
            idMap.entrySet().forEach(entry -> {
                String label = entry.getKey();
                List<Long> idList = entry.getValue();
                List<VertexProperty> vpList = new ArrayList<>();
                idList.forEach(id -> {
                    vpList.add(readVertexProperty(vertex, FireflyId.of(this, FireflyVertexProperty.class, id)));
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
        final Record vertexRecord = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex)).record;

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
        final FireflyRecord vertexRecord = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex));
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
                return new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().stream().map(vxp -> vxp.id()));
            }).collect(Collectors.toMap(it -> (String) it.getKey(), it -> (List<Object>) it.getValue()));

        final Bin vpCounterBin = new Bin(VP_COUNTER, Value.get(vpCounter));
        final Bin vertexPropertyIds = new Bin(VERTEX_PROPERTY_NAME_TO_ID, Value.get(propertyKeys));
        FireflyRecord.write(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex), vertexPropertyIds, vpCounterBin);
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
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, getElementPropertySet(element.getClass()), FireflyId.fromElement(element));
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
        return new FireflyProperty(element, key, readTypeHintedValueFromMap(getElementPropertySet(element.getClass()), FireflyId.fromElement(element), getElementPropertySet(element.getClass()), key));
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
     * Read a record from VERTEX_AERO_SET and return a constructed FireflyVertex
     *
     * @param graph    Graph handle
     * @param vertexId id of Vertex to read
     * @return Vertex to return
     */

    public FireflyVertex readVertex(final FireflyGraph graph, final FireflyId vertexId) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, VERTEX_AERO_SET, vertexId);
        if (fireflyRecord == null) {
            return null;
        }
        return new FireflyVertex(FireflyId.loadFromAerospike(this, FireflyVertex.class, fireflyRecord), fireflyRecord.record.getString("label"), graph);
    }

    /**
     * write a labeled Vertex record
     *
     * @param graph    handle to Graph instance
     * @param vertexId vertex id to write
     * @param label    vertex label to write
     */
    public void writeVertex(final FireflyGraph graph, final FireflyId vertexId, final String label) {
        final Bin labelBin = new Bin("label", Value.get(label));
        FireflyRecord.writeElement(this, VERTEX_AERO_SET, vertexId, labelBin);
    }

    /**
     * remove a Vertex Record
     *
     * @param graph    refrence to Graph
     * @param vertexId id of Vertex to remove
     */
    public void removeVertex(final FireflyGraph graph, final FireflyId vertexId) {
        final Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, vertexId);
        delete(key);
    }

    /**
     * Get the in-edge ids for a vertex
     * If the counter is less than the cache size, use the cache
     * else, query by scan
     *
     * @param vertex Vertex to read in edge ids from
     * @return Iterator of raw Ids
     */
    public Iterator<Object> getInEdgeIdsFromVertex(final FireflyVertex vertex) {
        FireflyRecord r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex));
        long edge_count = r.record.getLong(IN_EDGE_COUNTER);
        boolean cacheDisabled = r.record.getBoolean(CACHE_DISABLED);
        if (edge_count < ID_CACHE_SIZE && !cacheDisabled)
            return getXXXIdsFromVertexByCache(vertex, IN_EDGES);
        else
            return getInEdgeIdsFromVertexByScan(vertex);
    }

    /**
     * Get the out-edge ids for a vertex
     * If the counter is less than the cache size, use the cache
     * else, query by scan
     *
     * @param vertex Vertex to read out edge ids from
     * @return Iterator of raw Ids
     */
    public Iterator<Object> getOutEdgeIdsFromVertex(final FireflyVertex vertex) {
        FireflyRecord r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex));
        long edgeCount = r.record.getLong(OUT_EDGE_COUNTER);
        boolean cacheDisabled = r.record.getBoolean(CACHE_DISABLED);
        if (edgeCount < ID_CACHE_SIZE && !cacheDisabled)
            return getXXXIdsFromVertexByCache(vertex, OUT_EDGES);
        else
            return getOutEdgeIdsFromVertexByScan(vertex);
    }

    /**
     * Scan for and return the out direction ids associated with a vertex
     *
     * @param vertex Vertex to read out edge ids from
     * @return Iterator of raw Ids
     */
    public Iterator<Object> getOutEdgeIdsFromVertexByScan(final FireflyVertex vertex) {
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(Direction.OUT.name()),
                        Exp.val((Long) idToStorageType(vertex.id())))
        );

        return this.scanFilteredIdsInSet(EDGE_AERO_SET, exp);
    }


    /**
     * read an Id cache from a vertex, return it as a map of label to ids with label
     *
     * @param vertex  Vertex to read data from
     * @param mapName Bin name
     * @return Map of data
     */
    public Map<String, List<Long>> getXXXIdsFromVertexLabelMap(final FireflyVertex vertex, String mapName) {
        FireflyRecord r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex));
        Map<String, List<Long>> labelIds = (Map<String, List<Long>>) r.record.getMap(mapName);
        if (labelIds == null) {
            labelIds = new HashMap<>();
        }
        return labelIds;
    }

    /**
     * read an Id cache from a vertex
     *
     * @param vertex  Vertex to read data from
     * @param mapName Bin name
     * @return Iterator of ids
     */
    public Iterator<Object> getXXXIdsFromVertexByCache(final FireflyVertex vertex, String mapName) {
        return IteratorUtils.map(
                IteratorUtils.flatMap(getXXXIdsFromVertexLabelMap(vertex, mapName).entrySet().iterator(),
                        mapEntry -> mapEntry.getValue().iterator()),
                it -> (Object) it);
    }

    /**
     * Issue a scan query for all In direction edges associated with a vertex
     *
     * @param vertex vertex to read in edge ids from
     * @return Iterator of raw ids
     */
    public Iterator<Object> getInEdgeIdsFromVertexByScan(final FireflyVertex vertex) {
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(Direction.IN.name()),
                        Exp.val((Long) idToStorageType(vertex.id())))
        );

        return this.scanFilteredIdsInSet(EDGE_AERO_SET, exp);
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
     * Read an Edge by id
     *
     * @param graph  Graph handle
     * @param edgeId Edge id to read
     * @return Edge
     */
    public FireflyEdge readEdge(final FireflyGraph graph, final FireflyId edgeId) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, EDGE_AERO_SET, edgeId);
        if (fireflyRecord == null) {
            return null;
        }
        return new FireflyEdge(edgeId,
                fireflyRecord.record.getString("label"),
                FireflyId.of(this, FireflyVertex.class, fireflyRecord.record.getLong(Direction.OUT.name())),
                FireflyId.of(this, FireflyVertex.class, fireflyRecord.record.getLong(Direction.IN.name())), graph);
    }

    /**
     * Write an edge
     * The edge will form 1 record in the EDGE_AERO_SET set
     * properties are written to the property record associated with this edge ID in the property set
     *
     * @param graph     handle to Graph
     * @param edgeId    Id of Edge to write
     * @param label     label for Edge to write
     * @param inVertex  in Vertex for new Edge
     * @param outVertex out Vertex for new Edge
     * @param keyValues Edge properties
     */
    public void writeEdge(final FireflyGraph graph,
                          final FireflyId edgeId,
                          final String label,
                          final FireflyVertex outVertex,
                          final FireflyVertex inVertex,
                          final Object[] keyValues) {

        final Bin labelBin = new Bin("label", Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(idToStorageType(inVertex.id())));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(idToStorageType(outVertex.id())));

        FireflyRecord.writeElement(this, EDGE_AERO_SET, edgeId, labelBin, inVbin, outVBin);
        addEdgeToVertex(outVertex, edgeId, label, Direction.OUT);
        addEdgeToVertex(inVertex, edgeId, label, Direction.IN);

        final Iterator<Object> propIter = IteratorUtils.asIterator(keyValues);
        while (propIter.hasNext()) {
            final Object propKey = propIter.next();
            final Object propVal = propIter.next();
            writeProperty(edgeId, FireflyEdge.class, (String) propKey, propVal);
        }
    }

    /**
     * Add a VertexProperty to a Vertex
     *
     * @param vertex
     * @param vp
     */
    private void addVPToVertex(FireflyVertex vertex, FireflyVertexProperty vp) {

        Map<String, List<Long>> labelIds;
        final Record r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex)).record();
        if (r == null) {
            labelIds = new HashMap<>();
        } else {
            labelIds = (Map<String, List<Long>>) Optional.ofNullable(r.getMap(VERTEX_PROPERTY_NAME_TO_ID)).orElse(new HashMap<>());
        }
        if (labelIds == null) {
            labelIds = new HashMap<>();
        }
        long vpCounter = r.getLong(VP_COUNTER);
        final List<Long> ids = labelIds.getOrDefault(vp.key(), new ArrayList<>());
        if (vpCounter < ID_CACHE_SIZE)
            ids.add(((Number) vp.id()).longValue());
        vpCounter++;

        labelIds.put(vp.key(), ids);
        final Bin edgeData = new Bin(VERTEX_PROPERTY_NAME_TO_ID, Value.get(labelIds));
        final Bin edgeCounterBin = new Bin(VP_COUNTER, Value.get(vpCounter));
        FireflyRecord.write(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex), edgeData, edgeCounterBin);
    }

    /**
     * add an edge to a vertex
     *
     * @param vertex
     * @param edgeId
     * @param label
     * @param direction
     */
    private void addEdgeToVertex(FireflyVertex vertex, FireflyId edgeId, String label, Direction direction) {
        final String directionKey = direction == Direction.IN ? IN_EDGES : OUT_EDGES;
        final String counterKey = direction == Direction.IN ? IN_EDGE_COUNTER : OUT_EDGE_COUNTER;
        Map<String, List<Long>> labelEdges;
        final Record r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex)).record();
        if (r == null) {
            labelEdges = new HashMap<>();
        } else {
            labelEdges = (Map<String, List<Long>>) Optional.ofNullable(r.getMap(directionKey)).orElse(new HashMap<>());
        }
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        long edgeCounter = r.getLong(counterKey);
        final List<Long> edges = labelEdges.getOrDefault(label, new ArrayList<>());
        boolean cacheDisabled = r.getBoolean(CACHE_DISABLED);
        if (edgeCounter < ID_CACHE_SIZE)
            edges.add(((Number) edgeId.value()).longValue());
        else
            cacheDisabled = true;
        edgeCounter++;

        labelEdges.put(label, edges);
        final Bin edgeDataBin = new Bin(directionKey, Value.get(labelEdges));
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        final Bin cacheDisabledBin = new Bin(CACHE_DISABLED, Value.get(cacheDisabled));
        FireflyRecord.write(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex),
                edgeDataBin, edgeCounterBin, cacheDisabledBin);
    }

    /**
     * remove an Edge from its associated Vertex
     *
     * @param graph     Graph refrence
     * @param vertex    Vertex to remove edge from
     * @param edge      Edge to remove
     * @param direction Direction of edge
     */
    public void removeEdgeFromVertex(final FireflyGraph graph, final FireflyVertex vertex, final FireflyEdge edge, final Direction direction) {
        if (vertex == null)
            throw new NoSuchElementException(); //@todo transactions for edge removal
        FireflyRecord r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex));
        final String directionKey = direction == Direction.IN ? IN_EDGES : OUT_EDGES;
        final String counterKey = direction == Direction.IN ? IN_EDGE_COUNTER : OUT_EDGE_COUNTER;
        if (r == null)
            return;
        Map<String, List<Long>> labelEdges = (Map<String, List<Long>>) r.record.getMap(directionKey);
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        long edgeCounter = r.record.getLong(counterKey);
        if (edgeCounter > 0)
            edgeCounter--;
        if (edgeCounter == ID_CACHE_SIZE - 1) // if id set size within cache size, restore the cache
            labelEdges = getXXXIdsFromVertexLabelMap(vertex, directionKey);
        List<Long> edges = labelEdges.getOrDefault(edge.label(), new ArrayList<>());
        edges.remove(((Number) edge.id()).longValue());
        labelEdges.put(edge.label(), edges);
        final Bin edgeIdsBin = new Bin(directionKey, Value.get(labelEdges));
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        FireflyRecord.write(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex), edgeIdsBin, edgeCounterBin);
    }

    /**
     * remove an edge by id
     *
     * @param graph  Graph reference
     * @param edgeId Id of edge to remove
     */
    public void removeEdge(final FireflyGraph graph, final FireflyId edgeId) {
        final Key key = FireflyRecord.getKey(namespace, EDGE_AERO_SET, edgeId);
        FireflyEdge e = readEdge(graph, edgeId);
        if (e == null) //@todo transactions for edge removal
            return;
//            throw new NoSuchElementException();
        removeEdgeFromVertex(graph, (FireflyVertex) e.inVertex(), e, Direction.IN);
        removeEdgeFromVertex(graph, (FireflyVertex) e.outVertex(), e, Direction.OUT);
        delete(key);
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
        FireflyRecord.write(this, ID_MANAGER_SET, FireflyId.of(this, null, name), ctr);
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
    public void dropDatabase() {
        client.truncate(null, namespace, EDGE_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_PROPERTY_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, ID_MANAGER_SET, Calendar.getInstance());
        client.truncate(null, namespace, TEST_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_EDGELIST_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, GRAPH_VARIABLES_SET, Calendar.getInstance());
    }

    @Override
    public final String toString() {
        return String.format("aerospike://%s:%s/%s", host, port, namespace);
    }


    public Set<String> getIndexedKeys(Class<? extends FireflyElement> elementType) {
        //@todo getElementPropertySet is providing a string as the record key
        Key key = new Key(namespace, INDEX_METADATA, getElementPropertySet(elementType));
        Record rec = read(key);
        List<String> keys = (List<String>) rec.getList("indexedKeys");
        return new HashSet<>(keys);
    }

    public void dropIndex(
            final String set,
            final String indexName
    ) {
        final Policy policy = new Policy();
        policy.socketTimeout = 0; // Do not timeout on index create.
        try {
            final IndexTask task = client.dropIndex(policy, namespace, set, indexName);
            task.waitTillComplete();
        } catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw new RuntimeException(ae);
            }
        }
    }

    public void createIndex(
            final String set,
            final String indexName,
            final String binName,
            final IndexType type,
            final IndexCollectionType indexCollectionType
    ) {
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

    public <T extends Element> void createKeyIndex(Class<? extends FireflyElement> indexClass,
                                                   String key,
                                                   IndexType idxType,
                                                   IndexCollectionType idxColTypee) {
        Key mKey = new Key(namespace, INDEX_METADATA, getElementPropertySet(indexClass));
        Record rec = read(mKey);
        List<String> keys = (List<String>) rec.getList("indexedKeys");
        keys.add(key);
        Bin keysBin = new Bin("indexedKeys", new ArrayList<>(new HashSet<>(keys)));
        client.put(null, mKey, keysBin);
        createIndex(getElementPropertySet(indexClass), key, key, idxType, idxColTypee);
    }

    public <T extends Element> void dropKeyIndex(Class<? extends FireflyElement> indexClass, String key) {
        Key mKey = new Key(namespace, INDEX_METADATA, getElementPropertySet(indexClass));
        Record rec = read(mKey);
        List<String> keys = (List<String>) rec.getList("indexedKeys");
        keys.remove(key);
        Bin keysBin = new Bin("indexedKeys", new ArrayList<>(new HashSet<>(keys)));
        client.put(null, mKey, keysBin);
        dropIndex(getElementPropertySet(indexClass), key);
    }


    /**
     * close the connection to Aerospike
     */
    public void close() {
        this.client.close();
        this.eventLoops.close();
    }
}
