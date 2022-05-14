package com.aerospike.firefly.io;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListSortFlags;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.util.Exceptions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.*;
import java.util.stream.Collectors;

import static com.aerospike.firefly.util.Serialization.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class AerospikeConnection {
    private final String host;
    private final int port;
    private final AerospikeClient client;
    private final String namespace;

    private static final String EDGE_AERO_SET = FireflyEdge.class.getSimpleName().toUpperCase();
    private static final String VERTEX_AERO_SET = FireflyVertex.class.getSimpleName().toUpperCase();
    private static final String PROPERTY_AERO_SET = FireflyVertexProperty.class.getSimpleName().toUpperCase();
    private static final String VERTEX_PROPERTY_AERO_SET = FireflyVertexProperty.class.getSimpleName().toUpperCase();
    private static final String EDGE_ID_KEY = "_EDIDST";
    private static final String EDGE_ID_BIN = "_EDIDBN";
    private static final String VERTEX_ID_KEY = "_VXIDST";
    private static final String VERTEX_ID_BIN = "_VXIDBN";
    private static final String VERTEX_PROPERTY_ID_KEY = "_VPIDST";
    private static final String VERTEX_PROPERTY_ID_BIN = "_VPIDBN";
    private static final String VERTEX_PROPERTY_KEYS = "_VPK";
    private static final String ELEMENT_PROPERTIES = "_EP";
    public static final String GLOBAL = "_GLOBAL";
    private static final String COUNTER = "_COUNTER";
    private static final String ID_MANAGER_SET = "_IDMGR";


    private AerospikeConnection(final String host, final int port, final String namespace) {
        this.host = host;
        this.port = port;
        this.client = new AerospikeClient(host, port);
        this.namespace = namespace;
    }

    public static AerospikeConnection connect(final String host, final int port, final String namespace) {
        return new AerospikeConnection(host, port, namespace);
    }

    protected Record read(final Key key) {
        return client.get(null, key);
    }

    public void write(final Key key, final Bin... bins) {
        client.put(null, key, bins);
    }

    public void delete(final Key key) {
        client.delete(null, key);
    }

    public <V> List<VertexProperty> readVertexProperty(final FireflyVertex vertex, final String propertyKey) {
        final Key key = new Key(namespace, VERTEX_PROPERTY_AERO_SET, (Long) vertex.id());
        final Record r = read(key);
        if (r == null)
            return null;
        final List<String> propertyKeys = (List<String>) r.getList(VERTEX_PROPERTY_KEYS);
        if (propertyKeys == null || !propertyKeys.contains(propertyKey))
            return null;
        final List<byte[]> serializedProperties = (List<byte[]>) r.getList((String) propertyKey);

        return deserializeList(serializedProperties, VertexProperty.class);
    }

    public Map<String, List<VertexProperty>> readVertexProperties(final FireflyVertex vertex) {
        final Key key = new Key(namespace, VERTEX_PROPERTY_AERO_SET, (Long) vertex.id());
        final Record r = read(key);
        if (r == null)
            return new HashMap<>();

        final HashMap<String, List<VertexProperty>> result = new HashMap<>();
        final List<String> propertyKeys = (List<String>) r.getList(VERTEX_PROPERTY_KEYS);
        if (propertyKeys == null)
            return result;
        propertyKeys.forEach(vpk -> {
            List<byte[]> serializedProperties = (List<byte[]>) r.getList((String) vpk);
            result.put((String) vpk, deserializeList(serializedProperties, VertexProperty.class));
        });
        return result;
    }

    public void writeVertexProperty(final FireflyVertex vertex, final String k, final List<VertexProperty> v) {
        final Key key = new Key(namespace, VERTEX_PROPERTY_AERO_SET, (Long) vertex.id());
        final Record r = read(key);

        List<Value> propertyKeys = r == null ?
                new LinkedList<>() : (List<Value>) r.getList(VERTEX_PROPERTY_KEYS);
        if (propertyKeys == null)
            propertyKeys = new ArrayList<>();
        propertyKeys.add(Value.get(k));
        final ArrayList<Value> values =
                new ArrayList<>(v.stream().map(vp -> Value.get(serializeObject(vp))).collect(Collectors.toList()));
        final Bin propertyValues = new Bin(k, values);
        final Bin propertyKeysBin = new Bin(VERTEX_PROPERTY_KEYS, propertyKeys);
        write(key, propertyValues, propertyKeysBin);
    }

    public void removeVertexProperty(FireflyGraph graph, Object id) {
        final Key key = new Key(namespace, VERTEX_PROPERTY_AERO_SET, (Long) id);
        removeElementId(FireflyVertexProperty.class, id);
        delete(key);
    }

    public <V> Map<String, Property> readProperties(final FireflyElement ele) {
        final Key key = new Key(namespace, PROPERTY_AERO_SET, (Long) ele.id());
        final Record r = read(key);
        if (r == null)
            return new HashMap<>();

        final Map<String, Property> result = new HashMap<>();
        final Map<String, byte[]> data = (Map<String, byte[]>) r.getMap(ELEMENT_PROPERTIES);
        if (data == null)
            return result;
        data.entrySet().stream().forEach(e -> {
            final Map.Entry<String, byte[]> entry = (Map.Entry<String, byte[]>) e;
            Property prop = deserializeObject(entry.getValue(), Property.class);
            result.put(entry.getKey(), prop);
        });
        return result;
    }

    public <V> Property readProperty(final FireflyElement ele, final String k) {
        final Key key = new Key(namespace, PROPERTY_AERO_SET, (Long) ele.id());
        final Record r = read(key);
        return deserializeObject((byte[]) r.getMap(ELEMENT_PROPERTIES).get(k), Property.class);
    }

    public <V> void writeProperty(final FireflyElement ele, final String k, final Property<V> property) {
        final Key key = new Key(namespace, PROPERTY_AERO_SET, (Long) ele.id());
        final Record r = read(key);
        Map<String, byte[]> data;
        if (r == null)
            data = new HashMap<>();
        else
            data = (Map<String, byte[]>) Optional.ofNullable(r.getMap(ELEMENT_PROPERTIES)).orElse(new HashMap<>());
        data.put(k, serializeObject(property));
        final Bin bin = new Bin(ELEMENT_PROPERTIES, Value.get(data));
        write(key, bin);
    }

    public FireflyVertex readVertex(final FireflyGraph graph, final Object id) {
        final Key key = new Key(namespace, VERTEX_AERO_SET, (Long) id);
        final Record r = read(key);
        if (r == null) {
            return null;
        }
        return new FireflyVertex(r, id, r.getString("label"), graph);
    }

    public void writeVertex(final FireflyGraph graph, final Object id, final String label) {
        final Key key = new Key(namespace, VERTEX_AERO_SET, (Long) id);
        final Bin lbin = new Bin("label", Value.get(label));
        write(key, lbin);
    }

    public void removeVertex(FireflyGraph graph, Object id) {
        final Key key = new Key(namespace, VERTEX_AERO_SET, (Long) id);
        removeElementId(FireflyVertex.class, id);
        delete(key);
    }

    public List<Object> getInEdgeIdsFromVertex(final Record r) {
        final List<Object> inEdgeIds = (List<Object>) r.getList(Direction.IN.name());
        return inEdgeIds == null ? new LinkedList<>() : inEdgeIds;
    }

    public List<Object> getOutEdgeIdsFromVertex(final Record r) {
        List<Object> outEdgeIds = (List<Object>) r.getList(Direction.OUT.name());
        return outEdgeIds == null ? new LinkedList<>() : outEdgeIds;
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

    public void writeElementId(final Class<? extends FireflyElement> type, final Object id) {
        final id_config cfg = new id_config(type);
        final Key key = new Key(namespace, cfg.getAeroSet(), cfg.getIdKey());
        client.operate(client.writePolicyDefault, key,
                ListOperation.append(cfg.getIdBin(), Value.get(((Number) id).longValue())));
    }

    public void removeElementId(final Class<? extends FireflyElement> type, final Object id) {
        final id_config cfg = new id_config(type);
        final Key key = new Key(namespace, cfg.getAeroSet(), cfg.getIdKey());
        client.operate(client.writePolicyDefault, key,
                ListOperation.removeByValue(cfg.getIdBin(),
                        Value.get(((Number) id).longValue()), ListReturnType.NONE));
    }

    public Iterator<?> readElementIds(final Class<? extends FireflyElement> type) {
        final id_config cfg = new id_config(type);
        final Key key = new Key(namespace, cfg.getAeroSet(), cfg.getIdKey());
        final Record r = read(key);
        if (r == null || r.getList(cfg.getIdBin()).isEmpty())
            return EmptyIterator.instance();
        client.operate(client.writePolicyDefault, key,
                ListOperation.sort(cfg.getIdBin(), ListSortFlags.DROP_DUPLICATES)
        );
        return (Iterator<Object>) read(key).getList(cfg.getIdBin()).iterator();
    }

    public void addEdgeToVertex(final FireflyGraph graph, final Object vertexId, final Object edgeId, final Direction direction) {
        final Key key = new Key(namespace, VERTEX_AERO_SET, (Long) vertexId);
        final Record r = read(key);
        List<Long> directionEdges;
        directionEdges = (List<Long>) r.getList(direction.name());
        if (directionEdges == null) {
            directionEdges = new ArrayList<>();
        }
        directionEdges.add(((Number) edgeId).longValue());
        final Bin deb = new Bin(direction.name(), Value.get(directionEdges));
        write(key, deb);
    }

    public void removeEdgeFromVertex(final FireflyGraph graph, final Object vertexId, final Object edgeId, final Direction direction) {
        final Key key = new Key(namespace, VERTEX_AERO_SET, (Long) vertexId);
        final Record r = read(key);
        List<Long> directionEdges;
        directionEdges = (List<Long>) r.getList(direction.name());
        if (directionEdges == null) {
            directionEdges = new ArrayList<>();
        }
        directionEdges.remove(((Number) edgeId).longValue());
        final Bin deb = new Bin(direction.name(), Value.get(directionEdges));
        write(key, deb);
    }


    public long getIdCounter(final String name) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Record r = read(key);
        return r.getLong(COUNTER);
    }

    public long incrementIdCounter(final String name) {
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

    public void close() {
        this.client.close();
    }


    public FireflyEdge readEdge(final FireflyGraph graph, final Object id) {
        final Key key = new Key(namespace, EDGE_AERO_SET, (Long) id);
        final Record r = read(key);
        if (r == null) {
            return null;
        }
        return new FireflyEdge(r, id, r.getString("label"), r.getLong(Direction.IN.name()), r.getLong(Direction.OUT.name()), graph);
    }

    public void writeEdge(final FireflyGraph graph,
                          final Object id,
                          final String label,
                          final FireflyVertex inVertex,
                          final FireflyVertex outVertex,
                          final Object[] keyValues) {
        final Key key = new Key(namespace, EDGE_AERO_SET, (Long) id);
        final Bin lbin = new Bin("label", Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(inVertex.id()));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(outVertex.id()));
        write(key, lbin, inVbin, outVBin);
        addEdgeToVertex(graph, inVertex.id(), id, Direction.IN);
        addEdgeToVertex(graph, outVertex.id(), id, Direction.OUT);
    }

    public void removeEdge(final FireflyGraph graph, final Object id) {
        final Key key = new Key(namespace, EDGE_AERO_SET, (Long) id);
        removeElementId(FireflyEdge.class, id);
        delete(key);
    }

    public <V> void removePropertyFromVertex(final FireflyVertex vFireflyVertexProperty, final String key) {
        throw new Exceptions.Unimplemented();
    }

    public <V> void removePropertyFromVertexProperty(final FireflyVertexProperty vFireflyVertexProperty, final String key) {
        throw new Exceptions.Unimplemented();

    }

    public void removePropertyFromEdge(final FireflyEdge fireflyEdge, final String key) {
        throw new Exceptions.Unimplemented();
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", host, port, namespace);
    }

    public Map<String, Property> readEdgeProperties(final FireflyEdge edge) {
        return null;
    }

    public void dropDatabase() {
        client.truncate(null, namespace, EDGE_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, PROPERTY_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_PROPERTY_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, ID_MANAGER_SET, Calendar.getInstance());
    }
}
