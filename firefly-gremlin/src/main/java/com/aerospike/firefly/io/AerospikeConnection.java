package com.aerospike.firefly.io;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.async.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.util.ConfigurationHelper;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
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


    protected final String GRAPH_METADATA_SET;
    protected final String GRAPH_VARIABLES_SET;
    protected final String GRAPH_VARIABLES_RECORD;
    protected final String GRAPH_VARIABLES_MAP;
    protected final String EDGE_AERO_SET;
    protected final String VERTEX_AERO_SET;
    protected final String VERTEX_EDGELIST_AERO_SET;
    protected final String PROPERTY_AERO_SET;
    protected final String VERTEX_PROPERTY_AERO_SET;
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

    protected final String ON_RECORD_ID_LIMIT;
    protected final String VERTEX_PROPERTY_SET;
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


    private static Object idToStorageType(Object origId) {
        if (FireflyElement.class.isAssignableFrom(origId.getClass()))
            origId = ((FireflyElement) origId).id();
        if (Integer.class.equals(origId.getClass()))
            return ((Integer) origId).longValue();
        if (String.class.equals(origId.getClass()))
            return Long.parseLong((String) origId);
        return origId;
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

    private String getElementPropertySet(final Class<? extends FireflyElement> ele) {
        if (ele.equals(FireflyVertex.class))
            return VERTEX_PROPERTY_SET;
        else if (ele.equals(FireflyEdge.class))
            return EDGE_PROPERTIES;
        else if (ele.equals(FireflyVertexProperty.class))
            return VP_PROPERTIES;
        throw new UnsupportedOperationException("ele not supported " + ele.getClass());
    }

    private final int commandsPerLoop = 25;
    private final ClientPolicy clientPolicy;

    private Long getSupportedType(final Class clazz) {
        if (!SupportedValueTypes.containsKey(clazz))
            throw new UnsupportedOperationException(clazz.getName() + " is not a supported value type");
        return SupportedValueTypes.get(clazz);
    }

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
        PROPERTY_AERO_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.PROPERTY_AERO_SET, conf);
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
        VERTEX_PROPERTY_SET = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_PROPERTY_SET, conf);
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
        ON_RECORD_ID_LIMIT = ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, conf);


    }

    public static AerospikeConnection connect(final Configuration conf) {
        return new AerospikeConnection(conf);
    }

    Throttles initializeThrottles(final int numLoops, final int commandsPerEventLoop) {
        final Throttles throttles = new Throttles(numLoops, commandsPerEventLoop);
        return throttles;
    }

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


    protected Record read(final Key key) {
        return client.get(null, key);
    }


    protected boolean exists(final Key key) {
        return client.exists(null, key);
    }


    public void delete(final Key key) {
        client.delete(null, key);
    }


    public boolean vertexExists(final FireflyId fid) {
        final Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, fid);
        return exists(key);
    }

    public boolean edgeExists(final FireflyId fid) {
        final Key key = FireflyRecord.getKey(namespace, EDGE_AERO_SET, fid);
        return exists(key);
    }

    public Boolean vertexPropertyExists(final FireflyId fid) {
        final Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, fid);
        return exists(key);
    }

    public boolean aerospikeEnterprise() {
        return true; //@todo
    }


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

    protected Iterator<Long> scanAllIdsInSet(final String setName) {
        //@todo performance
        final Iterator<Map.Entry<Key, Record>> i = scanAllKeysInSet(setName, null);
        return IteratorUtils.map(i, keyRecordEntry -> {
            return keyRecordEntry.getKey().userKey.toLong();
        });
    }

    protected Iterator<Object> scanFilteredIdsInSet(final String setName, final Expression exp) {
        final Iterator<Map.Entry<Key, Record>> i = scanAllKeysInSet(setName, exp);
        return IteratorUtils.map(i, keyRecordEntry -> {
            return keyRecordEntry.getKey().userKey.getObject();
        });
    }

    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName) {
        return scanAllRecordsInSet(setName, null);
    }

    protected Iterator<Map.Entry<Key, Record>> scanAllKeysInSet(final String setName, final Expression exp, String... binNames) {
        ScanPolicy policy = new ScanPolicy();
        policy.includeBinData = false;
        return scanAllRecordsInSet(setName, exp, policy, binNames);
    }

    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName, final Expression exp, String... binNames) {
        return scanAllRecordsInSet(setName, exp, new ScanPolicy(), binNames);
    }

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

    private <V> void writeTypeHintedValueToMap(final String aeroSet,
                                               final FireflyId fid,
                                               final String mapName,
                                               final String mapKey,
                                               final V value) {
        writeTypeHintedValueToMap(aeroSet, fid, mapName, mapKey, value, null);
    }

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

    public <V> V readGraphVariable(final String k) {
        return readTypeHintedValueFromMap(GRAPH_VARIABLES_SET, FireflyId.of(this, null, GRAPH_VARIABLES_RECORD), GRAPH_VARIABLES_MAP, k);
    }

    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, GRAPH_VARIABLES_SET, FireflyId.of(this, null, GRAPH_VARIABLES_RECORD));
        if (fireflyRecord == null)
            return new HashSet<>();
        final Map<String, ?> m = (Map<String, ?>) fireflyRecord.record.getMap(GRAPH_VARIABLES_MAP);
        return m.keySet();
    }

    public <V> void writeGraphVariable(final String k, final V v) {
        writeTypeHintedValueToMap(GRAPH_VARIABLES_SET, FireflyId.of(this, null, GRAPH_VARIABLES_RECORD), GRAPH_VARIABLES_MAP, k, v);
    }

    public <V> void removeGraphVariable(final String k) {
        removeTypeHintedValueFromMap(GRAPH_VARIABLES_SET, FireflyId.of(this, null, GRAPH_VARIABLES_RECORD), GRAPH_VARIABLES_MAP, k);
    }


    /**
     * Read a single VertexProperty from its id
     *
     * @param vertex
     * @param fid
     * @param <V>
     * @return
     */
    public <V> VertexProperty<V> readVertexProperty(final FireflyVertex vertex, final FireflyId fid) {
        //@todo performance
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, VERTEX_PROPERTY_AERO_SET, fid);
        if (fireflyRecord == null)
            throw new NoSuchElementException();
        final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(readTypeHintedKeyValueFromMap(VERTEX_PROPERTY_AERO_SET, fid, KEY_VALUE));
        if (kv.isEmpty())
            return new FireflyVertexProperty<V>(fireflyRecord, fid, vertex, null, null);
        final String vpKey = kv.get().getKey();
        final Object vpVal = kv.get().getValue();
        return new FireflyVertexProperty<V>(fireflyRecord, fid, vertex, vpKey, (V) vpVal);
    }

    /**
     * Read the struct of vertex properties for an associated Vertex
     * the vertex record has a Map[String,List[ID]] inside it.
     * from this each VertexProperty is read from its own record by id
     *
     * @param vertex
     * @return
     */
    public Map<String, List<VertexProperty>> readVertexProperties(final FireflyVertex vertex) {
        final Object origId = vertex.id();
        final Long storageId = (Long) idToStorageType(origId);
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(PARENT_VERTEX_ID),
                        Exp.val(storageId))
        );
        final Iterator<Object> ids = scanFilteredIdsInSet(VERTEX_PROPERTY_AERO_SET, exp);
        //all results, empty
        final Map<String, List<VertexProperty>> results = new HashMap<>();
        //for every vp id associated with vertex
        ids.forEachRemaining(id -> {
            //load the vp
            final VertexProperty<Object> vp = readVertexProperty(vertex, FireflyId.of(this, FireflyVertexProperty.class, id));
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
     * Write a new vertex property
     *
     * @param vertex
     * @param fid
     * @param k
     * @param v
     * @param <V>
     */
    public <V> void writeVertexProperty(final FireflyVertex vertex,
                                        final FireflyId fid,
                                        final String vpk,
                                        final String k,
                                        final V v) {
        final Bin vpkBin = new Bin(VERTEX_PROPERTY_NAME, vpk);
        final Bin pviBin = new Bin(PARENT_VERTEX_ID, idToStorageType(vertex.id()));
        writeTypeHintedValueToMap(VERTEX_PROPERTY_AERO_SET, fid, KEY_VALUE, k, v, vpkBin, pviBin);
    }

    public void removeIdFromVertexPropertyList(final FireflyVertex vertex, final VertexProperty vp) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex));
        if (fireflyRecord == null)
            throw new NoSuchElementException();
        Map<String, List<Object>> propertyKeys = (Map<String, List<Object>>) fireflyRecord.record.getMap(VERTEX_PROPERTY_NAME_TO_ID);
        if (propertyKeys == null)
            propertyKeys = new HashMap<>();
        final List<Object> ids = propertyKeys.getOrDefault(vp.key(), new ArrayList<>());
        ids.remove(vp.id());
        if (ids.isEmpty())
            propertyKeys.remove(vp.key());
        else
            propertyKeys.put(vp.key(), ids);
        final Bin vertexPropertyIds = new Bin(VERTEX_PROPERTY_NAME_TO_ID, Value.get(propertyKeys));
        FireflyRecord.write(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex), vertexPropertyIds);
    }

    /**
     * Remove a VertexProperty Record from the database
     *
     * @param property
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
     * @param ele
     * @param <V>
     * @return
     */

    public <V> Map<String, Property> readProperties(final FireflyElement ele) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, PROPERTY_AERO_SET, FireflyId.fromElement(ele));
        if (fireflyRecord == null)
            return new HashMap<>();

        final Map<String, Property> result = new HashMap<>();
        final Map<String, V> data = (Map<String, V>) fireflyRecord.record.getMap(getElementPropertySet(ele.getClass()));
        if (data == null)
            return result;
        data.forEach((key1, value) -> {
            Property<V> prop = readProperty(ele, key1);
            result.put(key1, prop);
        });
        return result;
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * construct and return a Property from the value associated with k in the ELEMENT_PROPERTIES map
     *
     * @param ele
     * @param k
     * @param <V>
     * @return
     */
    public <V> Property readProperty(final FireflyElement ele, final String k) {
        return new FireflyProperty(ele, k, readTypeHintedValueFromMap(PROPERTY_AERO_SET, FireflyId.fromElement(ele), getElementPropertySet(ele.getClass()), k));
    }


    private Object typeCast(final Class clazz, final Object val) {
        if (clazz.equals(Integer.class))
            return Math.toIntExact((Long) val);
        return clazz.cast(val);
    }

    /**
     * write a new property into the property Record for element
     * 1 property record per element, a Map bin of name -> value
     *
     * @param id
     * @param k
     * @param value
     * @param <V>
     */
    public <V> void writeProperty(final FireflyId id, final Class<? extends FireflyElement> clazz, final String k, final V value) {
        FireflyHelper.validatePropertyValue(value);
        writeTypeHintedValueToMap(PROPERTY_AERO_SET, id, getElementPropertySet(clazz), k, value);
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * remove k from the ELEMENT_PROPERTIES map
     *
     * @param ele
     * @param k
     * @param <V>
     */
    public <V> void removeProperty(final FireflyElement ele, final String k) {
        removeTypeHintedValueFromMap(PROPERTY_AERO_SET, FireflyId.fromElement(ele), getElementPropertySet(ele.getClass()), k);
    }

    /**
     * Read a record from VERTEX_AERO_SET and return a constructed FireflyVertex
     *
     * @param graph
     * @param fid
     * @return
     */

    public FireflyVertex readVertex(final FireflyGraph graph, final FireflyId fid) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, VERTEX_AERO_SET, fid);
        if (fireflyRecord == null) {
            return null;
        }
        return new FireflyVertex(fireflyRecord, FireflyId.loadFromAerospike(this, FireflyVertex.class, fireflyRecord), fireflyRecord.record.getString("label"), graph);
    }

    /**
     * write a labeled Vertex record
     *
     * @param graph
     * @param fid
     * @param label
     */
    public void writeVertex(final FireflyGraph graph, final FireflyId fid, final String label) {
        final Bin labelBin = new Bin("label", Value.get(label));
        FireflyRecord.writeElement(this, VERTEX_AERO_SET, fid, labelBin);
    }

    /**
     * remove a Vertex Record
     *
     * @param graph
     * @param fid
     */
    public void removeVertex(final FireflyGraph graph, final FireflyId fid) {
        final Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, fid);
        delete(key);
    }

    public Iterator<Object> getInEdgeIdsFromVertex(final FireflyVertex v) {
        FireflyRecord r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(v));
        long edge_count = r.record.getLong(IN_EDGE_COUNTER);
        if (edge_count < Long.parseLong(ON_RECORD_ID_LIMIT))
            return getInEdgeIdsFromVertexByList(v);
        else
            return getInEdgeIdsFromVertexByScan(v);
    }

    public Iterator<Object> getOutEdgeIdsFromVertex(final FireflyVertex v) {
        FireflyRecord r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(v));
        long edge_count = r.record.getLong(OUT_EDGE_COUNTER);
        if (edge_count < Long.parseLong(ON_RECORD_ID_LIMIT))
            return getOutEdgeIdsFromVertexByList(v);
        else
            return getOutEdgeIdsFromVertexByScan(v);
    }

    public Iterator<Object> getOutEdgeIdsFromVertexByScan(final FireflyVertex v) {
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(Direction.OUT.name()),
                        Exp.val((Long) idToStorageType(v.id())))
        );

        return this.scanFilteredIdsInSet(EDGE_AERO_SET, exp);
    }

    public Iterator<Object> getOutEdgeIdsFromVertexByList(final FireflyVertex v) {
        FireflyRecord r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(v));

        Map<String, List<Long>> labelEdges = (Map<String, List<Long>>) r.record.getMap(OUT_EDGES);
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        return IteratorUtils.map(
                IteratorUtils.flatMap(labelEdges.entrySet().iterator(),
                        mapEntry -> mapEntry.getValue().iterator()),
                it -> (Object) it);
    }


    public Iterator<Object> getInEdgeIdsFromVertexByList(final FireflyVertex v) {
        FireflyRecord r = FireflyRecord.read(this, VERTEX_AERO_SET, FireflyId.fromElement(v));

        Map<String, List<Long>> labelEdges = (Map<String, List<Long>>) r.record.getMap(IN_EDGES);
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        return IteratorUtils.map(
                IteratorUtils.flatMap(labelEdges.entrySet().iterator(),
                        mapEntry -> mapEntry.getValue().iterator()),
                it -> (Object) it);
    }

    public Iterator<Object> getInEdgeIdsFromVertexByScan(final FireflyVertex v) {
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(Direction.IN.name()),
                        Exp.val((Long) idToStorageType(v.id())))
        );

        return this.scanFilteredIdsInSet(EDGE_AERO_SET, exp);
    }

    /**
     * get a list of currently valid ids
     *
     * @param type
     * @return
     */
    public Iterator<?> readElementIds(final Class<? extends FireflyElement> type) {
        final id_config cfg = new id_config(this, type);
        return scanAllIdsInSet(cfg.getAeroSet());
    }


    public FireflyEdge readEdge(final FireflyGraph graph, final FireflyId fid) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, EDGE_AERO_SET, fid);
        if (fireflyRecord == null) {
            return null;
        }
        return new FireflyEdge(fireflyRecord, fid,
                fireflyRecord.record.getString("label"),
                FireflyId.of(this, FireflyVertex.class, fireflyRecord.record.getLong(Direction.OUT.name())),
                FireflyId.of(this, FireflyVertex.class, fireflyRecord.record.getLong(Direction.IN.name())), graph);
    }

    /**
     * Write an edge
     * The edge will form 1 record in the EDGE_AERO_SET set
     * properties are written to the property record associated with this edge ID in the property set
     *
     * @param graph
     * @param fid
     * @param label
     * @param inVertex
     * @param outVertex
     * @param keyValues
     */
    public void writeEdge(final FireflyGraph graph,
                          final FireflyId fid,
                          final String label,
                          final FireflyVertex outVertex,
                          final FireflyVertex inVertex,
                          final Object[] keyValues) {

        final Bin labelBin = new Bin("label", Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(idToStorageType(inVertex.id())));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(idToStorageType(outVertex.id())));

        FireflyRecord.writeElement(this, EDGE_AERO_SET, fid, labelBin, inVbin, outVBin);
        addEdgeToVertex(outVertex, fid, label, Direction.OUT);
        addEdgeToVertex(inVertex, fid, label, Direction.IN);

        final Iterator<Object> propIter = IteratorUtils.asIterator(keyValues);
        while (propIter.hasNext()) {
            final Object propKey = propIter.next();
            final Object propVal = propIter.next();
            writeProperty(fid, FireflyEdge.class, (String) propKey, propVal);
        }
    }

    private void addEdgeToVertex(FireflyVertex vertex, FireflyId edgeId, String label, Direction direction) {
        final String directionKey = direction == Direction.IN ? IN_EDGES : OUT_EDGES;
        final String counterKey = direction == Direction.IN ? IN_EDGE_COUNTER : OUT_EDGE_COUNTER;

        Map<String, List<Long>> labelEdges;
        final Record r = FireflyRecord.read(this,VERTEX_AERO_SET,FireflyId.fromElement(vertex)).record();
        if (r == null) {
            labelEdges = new HashMap<>();
        } else {
            labelEdges = (Map<String, List<Long>>) Optional.ofNullable(r.getMap(directionKey)).orElse(new HashMap<>());
        }
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        long edgeCounter = r.getLong(counterKey);
        edgeCounter++;
        final List<Long> edges = labelEdges.getOrDefault(label, new ArrayList<>());
        edges.add(((Number) edgeId.value()).longValue());
        labelEdges.put(label, edges);
        final Bin edgeData = new Bin(directionKey, Value.get(labelEdges));
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        FireflyRecord.write(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex), edgeData, edgeCounterBin);
    }

    public void removeEdgeFromVertex(final FireflyGraph graph, final FireflyVertex vertex, final FireflyEdge edge, final Direction direction) {
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

        List<Long> edges = labelEdges.getOrDefault(edge.label(), new ArrayList<>());
        edges.remove(((Number) edge.id()).longValue());
        labelEdges.put(edge.label(), edges);
        final Bin edgeIdsBin = new Bin(directionKey, Value.get(labelEdges));
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        FireflyRecord.write(this, VERTEX_AERO_SET, FireflyId.fromElement(vertex), edgeIdsBin, edgeCounterBin);
    }


    public void removeEdge(final FireflyGraph graph, final FireflyId fid) {
        final Key key = FireflyRecord.getKey(namespace, EDGE_AERO_SET, fid);
        delete(key);
    }


    public long getIdCounter(final String name) {
        final Record record = read(new Key(namespace, ID_MANAGER_SET, name));
        return record.getLong(COUNTER);
    }

    public long incrementAndGetIdCounter(final String name, long increment) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, increment);
        final Record record = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return record.getLong(COUNTER);
    }

    public long incrementAndGetIdCounter(final String name) {
        return incrementAndGetIdCounter(name, 1);
    }

    public long decrementIdCounter(final String name) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, -1);
        final Record record = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return record.getLong(COUNTER);
    }

    public long zeroIdCounter(final String name) {
        final Bin ctr = new Bin(COUNTER, 0);
        FireflyRecord.write(this, ID_MANAGER_SET, FireflyId.of(this, null, name), ctr);
        return 0L;
    }

    //Offer a value, compare it to the current counter value.
    // if the offered value is greater then the current counter value
    // set the counter to the offered value, and return it.
    // otherwise, increment the counter by 1, and return that.
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

    public void dropDatabase() {
        client.truncate(null, namespace, EDGE_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, PROPERTY_AERO_SET, Calendar.getInstance());
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

    public void close() {
        this.client.close();
        this.eventLoops.close();
    }
}
