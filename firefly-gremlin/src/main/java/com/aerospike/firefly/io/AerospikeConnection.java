package com.aerospike.firefly.io;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.async.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.structure.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
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
    final int NumLoops = 2;
    final int CommandsPerEventLoop = 50;
    final int DelayQueueSize = 50;

    final EventLoops eventLoops;

    protected final String host;
    protected final int port;
    protected final AerospikeClient client;
    protected final String namespace;


    protected static final String GRAPH_METADATA_AERO_SET = "_GMST";
    protected static final String GRAPH_VARIABLES_AERO_SET = "_GVST";
    protected static final String GRAPH_VARIABLES_RECORD = "_GVR";
    protected static final String GRAPH_VARIABLES_MAP = "_GVM";
    protected static final String EDGE_AERO_SET = "_EDST";
    protected static final String VERTEX_AERO_SET = "_VXST";
    protected static final String VERTEX_EDGELIST_AERO_SET = "_VXEL";
    protected static final String PROPERTY_AERO_SET = "_PRST";
    protected static final String VERTEX_PROPERTY_AERO_SET = "_VPST";
    protected static final String EDGE_ID_KEY = "_EDIDST";
    protected static final String EDGE_ID_BIN = "_EDIDBN";
    protected static final String VERTEX_ID_KEY = "_VXIDST";
    protected static final String VERTEX_ID_BIN = "_VXIDBN";
    protected static final String VERTEX_PROPERTY_ID_KEY = "_VPIDST";
    protected static final String VERTEX_PROPERTY_ID_BIN = "_VPIDBN";
    protected static final String VERTEX_PROPERTY_NAME_TO_ID = "_VPK";
    protected static final String VERTEX_PROPERTY_NAME = "_VPN";
    protected static final String PARENT_VERTEX_ID = "_PVI";


    protected static final String VERTEX_PROPERTIES = "_VXP";
    protected static final String EDGE_PROPERTIES = "_EDP";
    protected static final String VP_PROPERTIES = "_VPP";
    protected static final String TYPE_HINTS = "_EPT";
    protected static final String KEY_VALUE = "_KV";
    protected static final String COUNTER = "_CT";
    protected static final String ID_MANAGER_SET = "_IDMGR";
    protected static final String ID_TYPE = "_IT";
    public static final String GLOBAL = "_GLOBAL";
    public static final String TEST_SET = "_TEST";


    private static Class<? extends Serializable> idTypeFromIdx(final long idx) {
        return SupportedIdTypes.entrySet().stream().filter(e -> e.getValue() == idx).collect(Collectors.toList()).get(0).getKey();
    }

    private static Object keyToStorageType(final Object origId) {
        if (Integer.class.equals(origId.getClass()))
            return ((Integer) origId).longValue();
        return origId;
    }

    private static Object idToStorageType(final Object origId) {
        if (Integer.class.equals(origId.getClass()))
            return ((Integer) origId).longValue();
        if (String.class.equals(origId.getClass()))
            return Long.parseLong((String) origId);
        return origId;
    }

    public static Object idStorageTypeToOriginalType(final Object storedId, final long originalTypeIdx) {
        return idStorageTypeToOriginalType(storedId, idTypeFromIdx(originalTypeIdx));
    }

    public static Object idStorageTypeToOriginalType(final Object storedId, final Class<? extends Serializable> origType) {
        if (origType.equals(Long.class))
            return storedId;
        if (origType.equals(Integer.class))
            return Math.toIntExact((Long) storedId);
        if (origType.equals(String.class)) //@todo separate string converted numeric from free string
            return storedId.toString();
        throw new UnsupportedOperationException(storedId.getClass() + " is not a supported id type");
    }

    public static final Map<Class<? extends Serializable>, Long> SupportedKeyTypes = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(byte[].class, 4L);
        put(String.class, 5L);
    }};
    public static final Map<Class<? extends Serializable>, Long> SupportedIdTypes = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(String.class, 5L);
    }};
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

    public static class FireflyRecord {
        public final Key key;
        public final Record record;
        public final Class<? extends Serializable> userClass;
        public final Class<? extends Serializable> storageClass;
        private static final WritePolicy sendKeyWritePolicy = new WritePolicy();

        static {
            sendKeyWritePolicy.sendKey = true;
        }


        private FireflyRecord(final Key key,
                              final Record record,
                              final Class<? extends Serializable> userClass,
                              final Class<? extends Serializable> storageClass) {
            this.key = key;
            this.record = record;
            this.userClass = userClass;
            this.storageClass = storageClass;
        }

        public Object id() {
            final long idval = key.userKey.toLong();
            final long idtypidx = record.getLong(ID_TYPE);
            return idStorageTypeToOriginalType(idval, idtypidx);
        }


        public static Key getKey(final String namespace, final String set, final Object id) {
            final Key key;
            if (id.getClass().equals(Long.class))
                key = new Key(namespace, set, (Long) id);
            else if (id.getClass().equals(Integer.class))
                key = new Key(namespace, set, (Long) keyToStorageType(id));
            else if (id.getClass().equals(String.class))
                key = new Key(namespace, set, (String) id);
            else if (id.getClass().equals(byte[].class))
                key = new Key(namespace, set, (byte[]) id);
            else
                throw new UnsupportedOperationException(id.getClass() + " unsuppored key type");
            return key;
        }

        public static Key getElementKey(final String namespace, final String set, final Object id) {
            final Key key;
            if (id.getClass().equals(Long.class))
                key = new Key(namespace, set, (Long) id);
            else if (id.getClass().equals(Integer.class))
                key = new Key(namespace, set, (Long) keyToStorageType(id));
            else if (id.getClass().equals(String.class))
                key = new Key(namespace, set, Long.parseLong((String) id));
            else if (id.getClass().equals(byte[].class))
                key = new Key(namespace, set, (byte[]) id);
            else
                throw new UnsupportedOperationException(id.getClass() + " unsuppored key type");
            return key;
        }

        public static FireflyRecord read(final AerospikeConnection db, final String set, final Object id) {
            final Key key = getKey(db.namespace, set, id);
            final Record record = db.read(key);
            if (record == null)
                return null;
            final long idTypeIdx = record.getLong(ID_TYPE);
            final Class<? extends Serializable> userClass = idTypeFromIdx(idTypeIdx);
            final Class<? extends Serializable> storageClass = KeyToDiskTypeMap.get(userClass);

            return new FireflyRecord(key, record, userClass, storageClass);
        }

        public static void write(final AerospikeConnection db,
                                 final String set,
                                 final Object idValue,
                                 final Bin... bins) {
            Long supportedIdTypeIdx = db.getSupportedKeyTypeIdx(idValue.getClass());
            final Key key = getKey(db.namespace, set, idValue);

            Bin idTypeBin = new Bin(ID_TYPE, Value.get(supportedIdTypeIdx));

            List<Bin> listOfBins = Arrays.stream(bins).collect(Collectors.toList());
            listOfBins.add(idTypeBin);

            try {
                db.client.put(sendKeyWritePolicy, key, listOfBins.toArray(new Bin[0]));
            } catch (com.aerospike.client.AerospikeException e) {
                throw new RuntimeException(e);
            }
        }

        public static void writeElement(final AerospikeConnection db,
                                        final String ns,
                                        final String set,
                                        final Object idValue,
                                        final Bin... bins) {
            Long supportedIdTypeIdx = db.getSupportedIdTypeIdx(idValue.getClass());
            final Key key = getElementKey(ns, set, idValue);
            Bin idTypeBin = new Bin(ID_TYPE, Value.get(supportedIdTypeIdx));
            List<Bin> listOfBins = Arrays.stream(bins).collect(Collectors.toList());
            listOfBins.add(idTypeBin);
            try {
                db.client.put(sendKeyWritePolicy, key, listOfBins.toArray(new Bin[0]));
            } catch (com.aerospike.client.AerospikeException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String getElementPropertySet(final FireflyElement ele) {
        if (ele.getClass().equals(FireflyVertex.class))
            return VERTEX_PROPERTIES;
        else if (ele.getClass().equals(FireflyEdge.class))
            return EDGE_PROPERTIES;
        else if (ele.getClass().equals(FireflyVertexProperty.class))
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

    private Long getSupportedKeyTypeIdx(final Class clazz) {
        if (!KeyToDiskTypeMap.containsKey(clazz))
            throw new UnsupportedOperationException(clazz.getName() + " is not a supported id type");
        return SupportedValueTypes.get(clazz);
    }

    private Long getSupportedIdTypeIdx(final Class clazz) {
        if (!IdToDiskTypeMap.containsKey(clazz))
            throw new UnsupportedOperationException(clazz.getName() + " is not a supported id type");
        return SupportedKeyTypes.get(clazz);
    }

    private AerospikeConnection(final String host, final int port, final String namespace) {
        this.host = host;
        this.port = port;
        this.eventLoops = initializeEventLoops(EventLoopType.DIRECT_NIO, NumLoops, CommandsPerEventLoop, DelayQueueSize);
        Host[] hosts = Host.parseHosts(host, port);
        this.clientPolicy = new ClientPolicy();
        this.clientPolicy.eventLoops = this.eventLoops;
        this.client = new AerospikeClient(clientPolicy, hosts);
        this.namespace = namespace;
    }

    Throttles initializeThrottles(final int numLoops, final int commandsPerEventLoop) {
        Throttles throttles = new Throttles(numLoops, commandsPerEventLoop);
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

    public static AerospikeConnection connect(final String host, final int port, final String namespace) {
        return new AerospikeConnection(host, port, namespace);
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


    public boolean vertexExists(final Object id) {
        Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, idToStorageType(id));
        return exists(key);
    }

    public boolean edgeExists(final Object id) {
        Key key = FireflyRecord.getKey(namespace, EDGE_AERO_SET, idToStorageType(id));
        return exists(key);
    }

    public Boolean vertexPropertyExists(final Object id) {
        Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, idToStorageType(id));
        return exists(key);
    }


    private static class id_config {
        private final Class<? extends FireflyElement> type;
        private final String AERO_SET;
        private final String ID_KEY;
        private final String ID_BIN;

        id_config(final Class<? extends FireflyElement> type) {
            this.type = type;
            if (type == FireflyVertex.class) {
                AERO_SET = VERTEX_AERO_SET;
                ID_KEY = VERTEX_ID_KEY;
                ID_BIN = VERTEX_ID_BIN;
            } else if (type == FireflyEdge.class) {
                AERO_SET = EDGE_AERO_SET;
                ID_KEY = EDGE_ID_KEY;
                ID_BIN = EDGE_ID_BIN;
            } else if (type == FireflyVertexProperty.class) {
                AERO_SET = VERTEX_PROPERTY_AERO_SET;
                ID_KEY = VERTEX_PROPERTY_ID_KEY;
                ID_BIN = VERTEX_PROPERTY_ID_BIN;
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
        Iterator<Map.Entry<Key, Record>> i = scanAllRecordsInSet(setName, null);
        return IteratorUtils.map(i, keyRecordEntry -> {
            return keyRecordEntry.getKey().userKey.toLong();
        });
    }

    protected Iterator<Object> scanFilteredIdsInSet(final String setName, final Expression exp) {
        //@todo performance can we filter out unnecessary bins?
        Iterator<Map.Entry<Key, Record>> i = scanAllRecordsInSet(setName, exp);
        return IteratorUtils.map(i, keyRecordEntry -> {
            return keyRecordEntry.getKey().userKey.getObject();
        });
    }

    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName) {
        return scanAllRecordsInSet(setName, null);
    }

    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName, final Expression exp) {
        Throttles throttles = initializeThrottles(this.eventLoops.getSize(), this.commandsPerLoop);
        Monitor scanMonitor = new Monitor();
        int progressFreq = 100;
        ScanPolicy policy = new ScanPolicy();
        policy.sendKey = true;
        if (exp != null)
            policy.filterExp = exp;
        ScanRecordSequenceListener listener = new ScanRecordSequenceListener(eventLoops,
                throttles,
                scanMonitor,
                client,
                progressFreq);
        client.scanAll(this.eventLoops.next(), listener, policy, this.namespace, setName);
        //@todo performance
        // should return custom iterator that produces results while query is running
        // custom iterator .hasNext() should return false once query is complete
        scanMonitor.waitTillComplete();

        return listener.iterator();
    }

    private <V> V readTypeHintedValueFromMap(final String aeroSet,
                                             final Object aeroKey,
                                             final String mapName,
                                             final String mapKey) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, aeroKey);
        if (fireflyRecord == null || !fireflyRecord.record.getMap(mapName).containsKey(mapKey))
            throw new NoSuchElementException();
        Optional<? extends Map<?, ?>> map = Optional.ofNullable(fireflyRecord.record.getMap(mapName));
        if (!map.isPresent())
            return null;
        Object val = map.get().get(mapKey);
        Long typeHint = (Long) fireflyRecord.record.getMap(TYPE_HINTS).get(mapKey);
        if (val == null)
            return null;
        Class clazz = SupportedValueTypes.entrySet()
                .stream()
                .filter(entry -> typeHint.equals(entry.getValue()))
                .map(Map.Entry::getKey).collect(Collectors.toList()).get(0);
        return (V) typeCast(clazz, val);
    }

    private <V> AbstractMap.Entry<String, V> readTypeHintedKeyValueFromMap(final String aeroSet,
                                                                           final Object aeroKey,
                                                                           final String mapName
    ) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, aeroKey);
        if (fireflyRecord == null || fireflyRecord.record.getMap(mapName).size() == 0)
            throw new NoSuchElementException();
        Optional<? extends Map<?, ?>> map = Optional.ofNullable(fireflyRecord.record.getMap(mapName));
        if (!map.isPresent())
            return null;
        String mapKey = (String) map.get().keySet().iterator().next();
        Long typeHint = (Long) fireflyRecord.record.getMap(TYPE_HINTS).get(mapKey);
        Object val = map.get().values().iterator().next();

        if (val == null)
            return null;
        Class clazz = SupportedValueTypes.entrySet()
                .stream()
                .filter(entry -> typeHint.equals(entry.getValue()))
                .map(Map.Entry::getKey).collect(Collectors.toList()).get(0);
        return new AbstractMap.SimpleEntry<>(mapKey, (V) typeCast(clazz, val));
    }


    private void removeTypeHintedValueFromMap(final String aeroSet,
                                              final Object aeroKey,
                                              final String mapName,
                                              final String mapKey) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, aeroKey);
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
        FireflyRecord.write(this, aeroSet, aeroKey, valueBin, typeHintBin);
    }

    private <V> void writeTypeHintedValueToMap(final String aeroSet,
                                               final Object aeroKey,
                                               final String mapName,
                                               final String mapKey,
                                               final V value) {
        writeTypeHintedValueToMap(aeroSet, aeroKey, mapName, mapKey, value, null);
    }

    private <V> void writeTypeHintedValueToMap(final String aeroSet,
                                               final Object aeroKey,
                                               final String mapName,
                                               final String mapKey,
                                               final V value, Bin... additionalBins) {
        final Map<String, Object> data;
        final Map<String, Object> typeHints;
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, aeroSet, aeroKey);
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
            FireflyRecord.write(this, aeroSet, aeroKey, valueBin, typeHintBin);
        } else {
            List<Bin> listOfBins = Arrays.stream(additionalBins).collect(Collectors.toList());
            listOfBins.add(valueBin);
            listOfBins.add(typeHintBin);
            FireflyRecord.write(this, aeroSet, aeroKey, listOfBins.toArray(new Bin[0]));
        }

    }

    public <V> V readGraphVariable(final String k) {
        return readTypeHintedValueFromMap(GRAPH_VARIABLES_AERO_SET, GRAPH_VARIABLES_RECORD, GRAPH_VARIABLES_MAP, k);
    }

    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, GRAPH_VARIABLES_AERO_SET, GRAPH_VARIABLES_RECORD);
        if (fireflyRecord == null)
            return new HashSet<>();
        final Map<String, ?> m = (Map<String, ?>) fireflyRecord.record.getMap(GRAPH_VARIABLES_MAP);
        return m.keySet();
    }

    public <V> void writeGraphVariable(final String k, final V v) {
        writeTypeHintedValueToMap(GRAPH_VARIABLES_AERO_SET, GRAPH_VARIABLES_RECORD, GRAPH_VARIABLES_MAP, k, v);
    }

    public <V> void removeGraphVariable(final String k) {
        removeTypeHintedValueFromMap(GRAPH_VARIABLES_AERO_SET, GRAPH_VARIABLES_RECORD, GRAPH_VARIABLES_MAP, k);
    }


    /**
     * Read a single VertexProperty from its id
     *
     * @param vertex
     * @param id
     * @param <V>
     * @return
     */
    public <V> VertexProperty<V> readVertexProperty(final FireflyVertex vertex, final Object id) {
        //@todo performance
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, VERTEX_PROPERTY_AERO_SET, id);
        if (fireflyRecord == null)
            throw new NoSuchElementException();
        final Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(readTypeHintedKeyValueFromMap(VERTEX_PROPERTY_AERO_SET, id, KEY_VALUE));
        if (kv.isEmpty())
            return new FireflyVertexProperty<V>(fireflyRecord, id, vertex, null, null);
        final String vpKey = kv.get().getKey();
        final Object vpVal = kv.get().getValue();
        return new FireflyVertexProperty<V>(fireflyRecord, id, vertex, vpKey, (V) vpVal);
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
            final VertexProperty<Object> vp = readVertexProperty(vertex, id);
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
     * @param id
     * @param k
     * @param v
     * @param <V>
     */
    public <V> void writeVertexProperty(final FireflyVertex vertex,
                                        final Object id,
                                        final String vpk,
                                        final String k,
                                        final V v) {
        final Bin vpkBin = new Bin(VERTEX_PROPERTY_NAME, vpk);
        final Bin pviBin = new Bin(PARENT_VERTEX_ID, idToStorageType(vertex.id()));
        writeTypeHintedValueToMap(VERTEX_PROPERTY_AERO_SET, id, KEY_VALUE, k, v, vpkBin, pviBin);
    }

    //@todo review property
    public void removeIdFromVertexPropertyList(final FireflyVertex vertex, final VertexProperty vp) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, VERTEX_AERO_SET, vertex.id());
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
        FireflyRecord.write(this, VERTEX_AERO_SET, vertex.id(), vertexPropertyIds);
    }

    /**
     * Remove a VertexProperty Record from the database
     *
     * @param property
     */
    public void removeVertexProperty(final FireflyVertexProperty property) {
        final Key key = FireflyRecord.getKey(namespace, VERTEX_PROPERTY_AERO_SET, property.id());
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
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, PROPERTY_AERO_SET, ele.id());
        if (fireflyRecord == null)
            return new HashMap<>();

        final Map<String, Property> result = new HashMap<>();
        final Map<String, V> data = (Map<String, V>) fireflyRecord.record.getMap(getElementPropertySet(ele));
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
        return new FireflyProperty(ele, k, readTypeHintedValueFromMap(PROPERTY_AERO_SET, ele.id(), getElementPropertySet(ele), k));
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
     * @param ele
     * @param k
     * @param value
     * @param <V>
     */
    public <V> void writeProperty(final FireflyElement ele, final String k, final V value) {
        FireflyHelper.validatePropertyValue(value);
        writeTypeHintedValueToMap(PROPERTY_AERO_SET, ele.id(), getElementPropertySet(ele), k, value);
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
        removeTypeHintedValueFromMap(PROPERTY_AERO_SET, ele.id(), getElementPropertySet(ele), k);
    }

    /**
     * Read a record from VERTEX_AERO_SET and return a constructed FireflyVertex
     *
     * @param graph
     * @param id
     * @return
     */

    public FireflyVertex readVertex(final FireflyGraph graph, final Object id) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, VERTEX_AERO_SET, idToStorageType(id));
        if (fireflyRecord == null) {
            return null;
        }
        return new FireflyVertex(fireflyRecord, id, fireflyRecord.record.getString("label"), graph);
    }

    /**
     * write a labeled Vertex record
     *
     * @param graph
     * @param id
     * @param label
     */
    public void writeVertex(final FireflyGraph graph, final Object id, final String label) {
        final Bin labelBin = new Bin("label", Value.get(label));
        FireflyRecord.writeElement(this, namespace, VERTEX_AERO_SET, id, labelBin);
    }

    /**
     * remove a Vertex Record
     *
     * @param graph
     * @param id
     */
    public void removeVertex(final FireflyGraph graph, final Object id) {
        final Key key = FireflyRecord.getKey(namespace, VERTEX_AERO_SET, id);
        delete(key);
    }


    public Iterator<Object> getOutEdgeIdsFromVertexByScan(final FireflyVertex v) {
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(Direction.OUT.name()),
                        Exp.val((Long) idToStorageType(v.id())))
        );

        return this.scanFilteredIdsInSet(EDGE_AERO_SET, exp);
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
        final id_config cfg = new id_config(type);
        return scanAllIdsInSet(cfg.getAeroSet());
    }


    public FireflyEdge readEdge(final FireflyGraph graph, final Object id) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(this, EDGE_AERO_SET, id);
        if (fireflyRecord == null) {
            return null;
        }
        return new FireflyEdge(fireflyRecord, id,
                fireflyRecord.record.getString("label"),
                fireflyRecord.record.getLong(Direction.OUT.name()),
                fireflyRecord.record.getLong(Direction.IN.name()), graph);
    }

    /**
     * Write an edge
     * The edge will form 1 record in the EDGE_AERO_SET set
     * properties are written to the property record associated with this edge ID in the property set
     *
     * @param graph
     * @param id
     * @param label
     * @param inVertex
     * @param outVertex
     * @param keyValues
     */
    public void writeEdge(final FireflyGraph graph,
                          final Object id,
                          final String label,
                          final FireflyVertex outVertex,
                          final FireflyVertex inVertex,
                          final Object[] keyValues) {

        final Bin labelBin = new Bin("label", Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(idToStorageType(inVertex.id())));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(idToStorageType(outVertex.id())));
        FireflyRecord.writeElement(this, namespace, EDGE_AERO_SET, id, labelBin, inVbin, outVBin);
        final FireflyEdge edge = readEdge(graph, id);
        final Iterator<Object> propIter = IteratorUtils.asIterator(keyValues);
        while (propIter.hasNext()) {
            final Object propKey = propIter.next();
            final Object propVal = propIter.next();
            writeProperty(edge, (String) propKey, propVal);
        }
    }

    public void removeEdge(final FireflyGraph graph, final Object id) {
        final Key key = FireflyRecord.getKey(namespace, EDGE_AERO_SET, id);
        delete(key);
    }


    public long getIdCounter(final String name) {
        final Record record = FireflyRecord.read(this, ID_MANAGER_SET, name).record;
        return record.getLong(COUNTER);
    }

    public long incrementAndGetIdCounter(final String name) {
        final Key key = FireflyRecord.getKey(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, 1);
        final Record record = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return record.getLong(COUNTER);
    }

    public long decrementIdCounter(final String name) {
        final Key key = FireflyRecord.getKey(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, -1);
        final Record record = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return record.getLong(COUNTER);
    }

    public long zeroIdCounter(final String name) {
        final Bin ctr = new Bin(COUNTER, 0);
        FireflyRecord.write(this, ID_MANAGER_SET, name, ctr);
        return 0L;
    }


    public void dropDatabase() {
        client.truncate(null, namespace, EDGE_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, PROPERTY_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_PROPERTY_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, ID_MANAGER_SET, Calendar.getInstance());
        client.truncate(null, namespace, TEST_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_EDGELIST_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, GRAPH_VARIABLES_AERO_SET, Calendar.getInstance());
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
