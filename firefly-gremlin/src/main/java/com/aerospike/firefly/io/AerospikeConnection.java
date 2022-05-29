package com.aerospike.firefly.io;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.async.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
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


    protected static final String ELEMENT_PROPERTIES = "_EP";
    protected static final String TYPE_HINTS = "_EPT";
    protected static final String KEY_VALUE = "_KV";
    protected static final String COUNTER = "_CT";
    protected static final String ID_MANAGER_SET = "_IDMGR";
    protected static final String ID_TYPE = "_IT";
    protected static final String KEY = "_K";
    public static final String GLOBAL = "_GLOBAL";
    public static final String TEST_SET = "_TEST";
    public static final Map<Class<? extends Serializable>, Long> SupportedTypes = new HashMap<>() {{
        put(String.class, 1L);
        put(Long.class, 2L);
        put(Integer.class, 3L);
        put(Boolean.class, 4L);
        put(ArrayList.class, 5L);
        put(Double.class, 6L);
        put(byte[].class, 7L);
    }};

    private Class<?> readGraphIdType() {
        Key key = new Key(namespace, GRAPH_METADATA_AERO_SET, KEY);
        Record record = read(key);
        if (record == null)
            return null;
        long type = record.getLong(ID_TYPE);
        if (type == 0)
            return null;
        return SupportedTypes.entrySet().stream().filter(it -> it.getValue() == type).collect(Collectors.toList()).get(0).getKey();
    }

    private void writeGraphIdType() {

    }

    private final int commandsPerLoop = 25;
    private final ClientPolicy clientPolicy;

    private Long getSupportedType(Class clazz) {
        if (!SupportedTypes.containsKey(clazz))
            throw new UnsupportedOperationException(clazz + " is not a supported type");
        return SupportedTypes.get(clazz);
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

    Throttles initializeThrottles(int numLoops, int commandsPerEventLoop) {
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

    protected void write(final Key key, final Bin... bins) {
        try {
            client.put(null, key, bins);
        } catch (com.aerospike.client.AerospikeException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean exists(Key key) {
        return client.exists(null, key);
    }


    public void delete(final Key key) {
        client.delete(null, key);
    }

    public boolean vertexExists(Object idValue) {
        Key key = new Key(namespace, VERTEX_AERO_SET, (Long) idValue);
        return client.exists(null, key);
    }

    public boolean edgeExists(Object idValue) {
        Key key = new Key(namespace, EDGE_AERO_SET, (Long) idValue);
        return client.exists(null, key);
    }

    public Boolean vertexPropertyExists(Object id) {
        return null;
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
            return keyRecordEntry.getValue().getLong(KEY);
        });
    }

    protected Iterator<Object> scanFilteredIdsInSet(final String setName, final Expression exp) {
        //@todo performance can we filter out unnecessary bins?
        Iterator<Map.Entry<Key, Record>> i = scanAllRecordsInSet(setName, exp);
        return IteratorUtils.map(i, keyRecordEntry -> {
            return keyRecordEntry.getValue().getLong(KEY);
        });
    }

    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName) {
        return scanAllRecordsInSet(setName, null);
    }

    protected Iterator<Map.Entry<Key, Record>> scanAllRecordsInSet(final String setName, Expression exp) {
        Throttles throttles = initializeThrottles(this.eventLoops.getSize(), this.commandsPerLoop);
        Monitor scanMonitor = new Monitor();
        int progressFreq = 100;
        ScanPolicy policy = new ScanPolicy();
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
        final Key key = getAeroKey(aeroSet, aeroKey);
        final Record r = read(key);
        if (r == null || !r.getMap(mapName).containsKey(mapKey))
            throw new NoSuchElementException();
        Optional<? extends Map<?, ?>> map = Optional.ofNullable(r.getMap(mapName));
        if (!map.isPresent())
            return null;
        Object val = map.get().get(mapKey);
        Long typeHint = (Long) r.getMap(TYPE_HINTS).get(mapKey);
        if (val == null)
            return null;
        Class clazz = SupportedTypes.entrySet()
                .stream()
                .filter(entry -> typeHint.equals(entry.getValue()))
                .map(Map.Entry::getKey).collect(Collectors.toList()).get(0);
        return (V) typeCast(clazz, val);
    }

    private <V> AbstractMap.Entry<String, V> readTypeHintedKeyValueFromMap(final String aeroSet,
                                                                           final Object aeroKey,
                                                                           final String mapName
    ) {
        final Key key = getAeroKey(aeroSet, aeroKey);
        final Record r = read(key);
        if (r == null || r.getMap(mapName).size() == 0)
            throw new NoSuchElementException();
        Optional<? extends Map<?, ?>> map = Optional.ofNullable(r.getMap(mapName));
        if (!map.isPresent())
            return null;
        String mapKey = (String) map.get().keySet().iterator().next();
        Long typeHint = (Long) r.getMap(TYPE_HINTS).get(mapKey);
        Object val = map.get().values().iterator().next();

        if (val == null)
            return null;
        Class clazz = SupportedTypes.entrySet()
                .stream()
                .filter(entry -> typeHint.equals(entry.getValue()))
                .map(Map.Entry::getKey).collect(Collectors.toList()).get(0);
        return new AbstractMap.SimpleEntry<>(mapKey, (V) typeCast(clazz, val));
    }

    private Key getAeroKey(String aeroSet, Object aeroKey) {
        final Key key;
        if (aeroKey.getClass().equals(String.class))
            key = new Key(namespace, aeroSet, (String) aeroKey);
        else if (aeroKey.getClass().equals(Long.class))
            key = new Key(namespace, aeroSet, (Long) aeroKey);
        else if (aeroKey.getClass().equals(Integer.class))
            key = new Key(namespace, aeroSet, (Long) aeroKey);
        else if (aeroKey.getClass().equals(byte[].class))
            key = new Key(namespace, aeroSet, (byte[]) aeroKey);
        else
            throw new UnsupportedOperationException("unsupported database record key class" + aeroKey.getClass());
        return key;
    }

    private void removeTypeHintedValueFromMap(final String aeroSet,
                                              final Object aeroKey,
                                              final String mapName,
                                              final String mapKey) {
        final Key key = getAeroKey(aeroSet, aeroKey);
        final Record r = read(key);
        final Map<String, Object> data;
        final Map<String, Object> typeHints;
        if (r == null)
            return;
        else {
            data = (Map<String, Object>) Optional.ofNullable(r.getMap(mapName)).orElse(new HashMap<>());
            typeHints = (Map<String, Object>) Optional.ofNullable(r.getMap(TYPE_HINTS)).orElse(new HashMap<>());
        }
        if (!data.containsKey(mapKey)) {
            return;
        } else {
            data.remove(mapKey);
            typeHints.remove(mapKey);
        }
        final Bin typeHintBin = new Bin(TYPE_HINTS, Value.get(typeHints));
        final Bin valueBin = new Bin(mapName, Value.get(data));
        write(key, valueBin, typeHintBin);
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

        final Record r = read(getAeroKey(aeroSet, aeroKey));
        final Map<String, Object> data;
        final Map<String, Object> typeHints;
        if (r == null) {
            data = new HashMap<>();
            typeHints = new HashMap<>();
        } else {
            data = (Map<String, Object>) Optional.ofNullable(r.getMap(mapName)).orElse(new HashMap<>());
            typeHints = (Map<String, Object>) Optional.ofNullable(r.getMap(TYPE_HINTS)).orElse(new HashMap<>());
        }
        if (value != null)
            typeHints.put(mapKey, getSupportedType(value.getClass()));
        else
            typeHints.put(mapKey, null);
        data.put(mapKey, value);
        final Bin typeHintBin = new Bin(TYPE_HINTS, Value.get(typeHints));
        final Bin valueBin = new Bin(mapName, Value.get(data));
        if (additionalBins == null)
            write(getAeroKey(aeroSet, aeroKey), valueBin, typeHintBin);
        else {
            List<Bin> listOfBins = Arrays.stream(additionalBins).collect(Collectors.toList());
            listOfBins.add(valueBin);
            listOfBins.add(typeHintBin);
            write(getAeroKey(aeroSet, aeroKey), listOfBins.toArray(new Bin[0]));
        }

    }

    public <V> V readGraphVariable(String k) {
        return readTypeHintedValueFromMap(GRAPH_VARIABLES_AERO_SET, GRAPH_VARIABLES_RECORD, GRAPH_VARIABLES_MAP, k);
    }

    public Set<String> readGraphVariableKeys() {
        Key key = new Key(this.namespace, GRAPH_VARIABLES_AERO_SET, GRAPH_VARIABLES_RECORD);
        Record r = read(key);
        if (r == null)
            return new HashSet<>();
        Map<String, ?> m = (Map<String, ?>) r.getMap(GRAPH_VARIABLES_MAP);
        return m.keySet();
    }

    public <V> void writeGraphVariable(String k, V v) {
        writeTypeHintedValueToMap(GRAPH_VARIABLES_AERO_SET, GRAPH_VARIABLES_RECORD, GRAPH_VARIABLES_MAP, k, v);
    }

    public <V> void removeGraphVariable(String k) {
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
        final Key key = new Key(namespace, VERTEX_PROPERTY_AERO_SET, (Long) id);
        final Record r = read(key);
        if (r == null)
            throw new NoSuchElementException();
        Optional<Map.Entry<String, Object>> kv = Optional.ofNullable(readTypeHintedKeyValueFromMap(VERTEX_PROPERTY_AERO_SET, id, KEY_VALUE));
        if (kv.isEmpty())
            return new FireflyVertexProperty<V>(id, vertex, null, null);
        String vpKey = kv.get().getKey();
        Object vpVal = kv.get().getValue();
        return new FireflyVertexProperty<V>(id, vertex, vpKey, (V) vpVal);

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
        final Key vertexKey = new Key(namespace, VERTEX_AERO_SET, (Long) vertex.id());
        final Record vertexRecord = read(vertexKey);
        if (vertexRecord == null)
            return new HashMap<>();

        final HashMap<String, List<VertexProperty>> result = new HashMap<>();
        Map<String, List<Object>> propertyKeys = (Map<String, List<Object>>) vertexRecord.getMap(VERTEX_PROPERTY_NAME_TO_ID);
        if (propertyKeys == null)
            propertyKeys = new HashMap<>();
        propertyKeys.forEach((k, v) -> {
            final List<VertexProperty> props = new ArrayList<>();
            v.forEach(id -> {
                props.add(readVertexProperty(vertex, id));
            });
            result.put(k, props);
        });
        return result;
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
    public <V> void writeVertexProperty(final FireflyVertex vertex, Object id, String vpk, String k, V v) {
        Bin idBin = new Bin(KEY, id);
        Bin vpkBin = new Bin(VERTEX_PROPERTY_NAME, vpk);
        Bin pviBin = new Bin(PARENT_VERTEX_ID, vertex.id());
        writeTypeHintedValueToMap(VERTEX_PROPERTY_AERO_SET, id, KEY_VALUE, k, v, idBin, vpkBin, pviBin);
    }

    /**
     * Write a new Key : List[VertexProperty] on the associated vertex
     *
     * @param vertex
     * @param vpk
     * @param listOfVP
     */

    public void writeVertexPropertyList(final FireflyVertex vertex, final String vpk, final List<VertexProperty> listOfVP) {
        final Key vertexKey = new Key(namespace, VERTEX_AERO_SET, (Long) vertex.id());
        final Record vertexRecord = read(vertexKey);
        if (vertexRecord == null)
            throw new NoSuchElementException();
        Map<String, List<Object>> propertyKeys = (Map<String, List<Object>>) vertexRecord.getMap(VERTEX_PROPERTY_NAME_TO_ID);
        if (propertyKeys == null)
            propertyKeys = new HashMap<>();
        final List<Object> ids = propertyKeys.getOrDefault(vpk, new ArrayList<>());

        listOfVP.forEach(vp -> {
            writeVertexProperty(vertex, vp.id(), vpk, vp.key(), vp.value());
            ids.add(vp.id());
        });

        final HashSet<Object> uniqueIds = new HashSet<>(ids);

        propertyKeys.put(vpk, Arrays.asList(uniqueIds.toArray()));

        final Bin vertexPropertyIds = new Bin(VERTEX_PROPERTY_NAME_TO_ID, Value.get(propertyKeys));
        write(vertexKey, vertexPropertyIds);
    }

    public void removeIdFromVertexPropertyList(final FireflyVertex vertex, VertexProperty vp) {
        final Key vertexKey = new Key(namespace, VERTEX_AERO_SET, (Long) vertex.id());
        final Record vertexRecord = read(vertexKey);
        if (vertexRecord == null)
            throw new NoSuchElementException();
        Map<String, List<Object>> propertyKeys = (Map<String, List<Object>>) vertexRecord.getMap(VERTEX_PROPERTY_NAME_TO_ID);
        if (propertyKeys == null)
            propertyKeys = new HashMap<>();
        final List<Object> ids = propertyKeys.getOrDefault(vp.key(), new ArrayList<>());

        ids.remove(vp.id());
        if (ids.isEmpty())
            propertyKeys.remove(vp.key());
        else
            propertyKeys.put(vp.key(), ids);
        final Bin vertexPropertyIds = new Bin(VERTEX_PROPERTY_NAME_TO_ID, Value.get(propertyKeys));
        write(vertexKey, vertexPropertyIds);
    }

    /**
     * Remove a VertexProperty Record from the database
     *
     * @param property
     */
    public void removeVertexProperty(FireflyVertexProperty property) {
        final Key key = new Key(namespace, VERTEX_PROPERTY_AERO_SET, (Long) property.id());
        Vertex parent = property.element();
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
        final Key key = new Key(namespace, PROPERTY_AERO_SET, (Long) ele.id());
        final Record r = read(key);
        if (r == null)
            return new HashMap<>();

        final Map<String, Property> result = new HashMap<>();
        final Map<String, V> data = (Map<String, V>) r.getMap(ELEMENT_PROPERTIES);
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
        return new FireflyProperty(ele, k, readTypeHintedValueFromMap(PROPERTY_AERO_SET, ele.id(), ELEMENT_PROPERTIES, k));
    }


    private Object typeCast(Class clazz, Object val) {
        if (clazz.equals(Integer.class))
            return Math.toIntExact((Long) val);
        return clazz.cast(val);
    }

    /**
     * write a new property into the property Record for element
     * 1 property record per element, a Map bin of name -> value
     *
     * @param element
     * @param k
     * @param value
     * @param <V>
     */
    public <V> void writeProperty(final FireflyElement element, final String k, final V value) {
        FireflyHelper.validatePropertyValue(value);
        writeTypeHintedValueToMap(PROPERTY_AERO_SET, element.id(), ELEMENT_PROPERTIES, k, value);
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * remove k from the ELEMENT_PROPERTIES map
     *
     * @param element
     * @param k
     * @param <V>
     */
    public <V> void removeProperty(final FireflyElement element, final String k) {
        removeTypeHintedValueFromMap(PROPERTY_AERO_SET, element.id(), ELEMENT_PROPERTIES, k);
    }

    /**
     * Read a record from VERTEX_AERO_SET and return a constructed FireflyVertex
     *
     * @param graph
     * @param id
     * @return
     */

    public FireflyVertex readVertex(final FireflyGraph graph, final Object id) {
        final Key key = new Key(namespace, VERTEX_AERO_SET, (Long) id);
        final Record r = read(key);
        if (r == null) {
            return null;
        }
        return new FireflyVertex(r, id, r.getString("label"), graph);
    }

    /**
     * write a labeled Vertex record
     *
     * @param graph
     * @param id
     * @param label
     */
    public void writeVertex(final FireflyGraph graph, final Object id, final String label) {
        final Key key = new Key(namespace, VERTEX_AERO_SET, (Long) id);
        final Bin keyBin = new Bin(KEY, id);
        final Bin lbin = new Bin("label", Value.get(label));
        write(key, keyBin, lbin);
    }

    /**
     * remove a Vertex Record
     *
     * @param graph
     * @param id
     */
    public void removeVertex(FireflyGraph graph, Object id) {
        final Key key = new Key(namespace, VERTEX_AERO_SET, (Long) id);
        delete(key);
    }


    public Iterator<Object> getOutEdgeIdsFromVertexByScan(final FireflyVertex v) {
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(Direction.OUT.name()),
                        Exp.val((Long) v.id()))
        );

        return this.scanFilteredIdsInSet(EDGE_AERO_SET, exp);
    }

    public Iterator<Object> getInEdgeIdsFromVertexByScan(final FireflyVertex v) {
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(Direction.IN.name()),
                        Exp.val((Long) v.id()))
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
        final Key key = new Key(namespace, EDGE_AERO_SET, (Long) id);
        final Record r = read(key);
        if (r == null) {
            return null;
        }
        return new FireflyEdge(r, id, r.getString("label"), r.getLong(Direction.IN.name()), r.getLong(Direction.OUT.name()), graph);
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
                          final FireflyVertex inVertex,
                          final FireflyVertex outVertex,
                          final Object[] keyValues) {

        final Key key = new Key(namespace, EDGE_AERO_SET, (Long) id);
        final Bin keyBin = new Bin(KEY, id);
        final Bin lbin = new Bin("label", Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(inVertex.id()));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(outVertex.id()));
        write(key, keyBin, lbin, inVbin, outVBin);
        FireflyEdge edge = readEdge(graph, id);
//        addEdgeToVertex(graph, inVertex.id(), edge, Direction.IN);
//        addEdgeToVertex(graph, outVertex.id(), edge, Direction.OUT);
        Iterator<Object> propIter = IteratorUtils.asIterator(keyValues);
        while (propIter.hasNext()) {
            Object propKey = propIter.next();
            Object propVal = propIter.next();
            writeProperty(edge, (String) propKey, propVal);
        }
    }

    public void removeEdge(final FireflyGraph graph, final Object id) {
        final Key key = new Key(namespace, EDGE_AERO_SET, (Long) id);
        delete(key);
    }


    public long getIdCounter(final String name) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Record r = read(key);
        return r.getLong(COUNTER);
    }

    public long incrementAndGetIdCounter(final String name) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, 1);
        Record r = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return r.getLong(COUNTER);
    }

    public long decrementIdCounter(final String name) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, -1);
        final Record r = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return r.getLong(COUNTER);
    }

    public long zeroIdCounter(final String name) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, 0);
        write(key, ctr);
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
