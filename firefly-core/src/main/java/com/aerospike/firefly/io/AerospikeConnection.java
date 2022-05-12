package com.aerospike.firefly.io;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.util.Exceptions;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;
import java.util.stream.Collectors;

import static com.aerospike.firefly.util.Serialization.*;
import static com.aerospike.firefly.util.Tokens.COUNTER;
import static com.aerospike.firefly.util.Tokens.ID_MANAGER_SET;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class AerospikeConnection {
    private final String host;
    private final int port;
    private final AerospikeClient client;
    private final String namespace;

    private AerospikeConnection(final String host, final int port, final String namespace) {
        this.host = host;
        this.port = port;
        this.client = new AerospikeClient(host, port);
        this.namespace = namespace;
    }

    public static AerospikeConnection connect(final String host, final int port, final String namespace) {
        return new AerospikeConnection(host, port, namespace);
    }

    public Record read(Key key) {
        return client.get(null, key);
    }

    public void write(Key key, Bin... bins) {
        client.put(null, key, bins);
    }

    public <V> List<VertexProperty> readVertexProperty(FireflyVertex vertex, String propertyKey) {
        Key key = new Key(namespace, FireflyVertexProperty.AERO_SET, (Long) vertex.id());
        Record r = read(key);
        if (r == null)
            return null;
        List<String> propertyKeys = (List<String>) r.getList(FireflyVertexProperty.VERTEX_PROPERTY_KEYS);
        if (propertyKeys == null || !propertyKeys.contains(propertyKey))
            return null;
        List<byte[]> serializedProperties = (List<byte[]>) r.getList((String) propertyKey);

        return deserializeList(serializedProperties, VertexProperty.class);
    }

    public Map<String, List<VertexProperty>> readVertexProperties(FireflyVertex vertex) {
        Key key = new Key(namespace, FireflyVertexProperty.AERO_SET, (Long) vertex.id());
        Record r = read(key);
        if (r == null)
            return new HashMap<>();

        HashMap<String, List<VertexProperty>> result = new HashMap<>();
        List<String> propertyKeys = (List<String>) r.getList(FireflyVertexProperty.VERTEX_PROPERTY_KEYS);
        if (propertyKeys == null)
            return result;
        propertyKeys.forEach(vpk -> {
            List<byte[]> serializedProperties = (List<byte[]>) r.getList((String) vpk);
            result.put((String) vpk, deserializeList(serializedProperties, VertexProperty.class));
        });
        return result;
    }

    public void writeVertexProperty(FireflyVertex vertex, String k, List<VertexProperty> v) {
        Key key = new Key(namespace, FireflyVertexProperty.AERO_SET, (Long) vertex.id());
        Record r = read(key);

        List<Value> propertyKeys = r == null ?
                new LinkedList<>() : (List<Value>) r.getList(FireflyVertexProperty.VERTEX_PROPERTY_KEYS);
        if (propertyKeys == null)
            propertyKeys = new ArrayList<>();
        propertyKeys.add(Value.get(k));
        ArrayList<Value> values =
                new ArrayList<>(v.stream().map(vp -> Value.get(serializeObject(vp))).collect(Collectors.toList()));
        Bin propertyValues = new Bin(k, values);
        Bin propertyKeysBin = new Bin(FireflyVertexProperty.VERTEX_PROPERTY_KEYS, propertyKeys);
        write(key, propertyValues, propertyKeysBin);
    }


    public <V> Map<String, Property> readProperties(FireflyElement ele) {
        Key key = new Key(namespace, FireflyProperty.AERO_SET, (Long) ele.id());
        Record r = read(key);
        if (r == null)
            return new HashMap<>();

        Map<String, Property> result = new HashMap<>();
        Map<String, byte[]> data = (Map<String, byte[]>) r.getMap(FireflyProperty.ELEMENT_PROPERTIES);
        if (data == null)
            return result;
        data.entrySet().stream().forEach(e -> {
            final Map.Entry<String, byte[]> entry = (Map.Entry<String, byte[]>) e;
            Property prop = deserializeObject(entry.getValue(), Property.class);
            result.put(entry.getKey(), prop);
        });
        return result;
    }

    public <V> Property readProperty(FireflyElement ele, String k) {
        Key key = new Key(namespace, FireflyProperty.AERO_SET, (Long) ele.id());
        Record r = read(key);
        return deserializeObject((byte[]) r.getMap(FireflyProperty.ELEMENT_PROPERTIES).get(k), Property.class);
    }

    public <V> void writeProperty(FireflyElement ele, String k, Property<V> property) {
        Key key = new Key(namespace, FireflyProperty.AERO_SET, (Long) ele.id());
        Record r = read(key);
        Map<String, byte[]> data = (Map<String, byte[]>) r.getMap(FireflyProperty.ELEMENT_PROPERTIES);
        if (data == null)
            data = new HashMap<>();
        data.put(k, serializeObject(property));
        Bin bin = new Bin(FireflyProperty.ELEMENT_PROPERTIES, Value.get(data));
        write(key, bin);
    }

    public FireflyVertex readVertex(FireflyGraph graph, Object id) {
        Key key = new Key(namespace, FireflyVertex.AERO_SET, (Long) id);
        Record r = read(key);
        if (r == null) {
            return null;
        }
        return new FireflyVertex(r, id, r.getString("label"), graph);

    }

    public void writeVertex(FireflyGraph fireflyGraph, Object id, String label) {
        Key key = new Key(namespace, FireflyVertex.AERO_SET, (Long) id);
        Bin lbin = new Bin("label", Value.get(label));
        write(key, lbin);
    }

    public void removeVertex(FireflyGraph fireflyGraph, Object id) {

        throw new Exceptions.Unimplemented();
    }

    public long getIdCounter(String name) {
        Key key = new Key(namespace, ID_MANAGER_SET, name);
        Record r = read(key);
        return r.getLong(COUNTER);
    }

    public long incrementIdCounter(String name) {
        Key key = new Key(namespace, ID_MANAGER_SET, name);
        Bin ctr = new Bin(COUNTER, 1);
        Record r = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return r.getLong(COUNTER);
    }

    public long decrementIdCounter(String name) {
        Key key = new Key(namespace, ID_MANAGER_SET, name);
        Bin ctr = new Bin(COUNTER, -1);
        Record r = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return r.getLong(COUNTER);
    }

    public long zeroIdCounter(String name) {
        Key key = new Key(namespace, ID_MANAGER_SET, name);
        Bin ctr = new Bin(COUNTER, 0);
        write(key, ctr);
        return 0L;
    }

    public void close() {
        this.client.close();
    }

    public Edge readEdge(FireflyGraph graph, Object id) {
        throw new Exceptions.Unimplemented();
    }

}
