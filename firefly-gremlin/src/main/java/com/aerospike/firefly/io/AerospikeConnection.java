package com.aerospike.firefly.io;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListSortFlags;
import com.aerospike.firefly.structure.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class AerospikeConnection {
    private final String host;
    private final int port;
    private final AerospikeClient client;
    private final String namespace;


    private static final String EDGE_AERO_SET = "_EDST";
    private static final String VERTEX_AERO_SET = "_VXST";
    private static final String VERTEX_EDGELIST_AERO_SET = "_VXEL";
    private static final String PROPERTY_AERO_SET = "_PRST";
    private static final String VERTEX_PROPERTY_AERO_SET = "_VPST";
    private static final String EDGE_ID_KEY = "_EDIDST";
    private static final String EDGE_ID_BIN = "_EDIDBN";
    private static final String VERTEX_ID_KEY = "_VXIDST";
    private static final String VERTEX_ID_BIN = "_VXIDBN";
    private static final String VERTEX_PROPERTY_ID_KEY = "_VPIDST";
    private static final String VERTEX_PROPERTY_ID_BIN = "_VPIDBN";
    private static final String VERTEX_PROPERTY_NAME_TO_ID = "_VPK";
    private static final String ELEMENT_PROPERTIES = "_EP";
    private static final String KEY_VALUE = "_KV";
    private static final String COUNTER = "_CT";
    private static final String ID_MANAGER_SET = "_IDMGR";
    private static final String LABEL_EDGES = "_LBLED";
    public static final String GLOBAL = "_GLOBAL";
    public static final String TEST_SET = "_TEST";


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
        Map<String, V> kv = (Map<String, V>) r.getMap(KEY_VALUE);
        String vpKey = kv.entrySet().iterator().next().getKey();
        Object vpVal = kv.entrySet().iterator().next().getValue();

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
    public <V> void writeVertexProperty(final FireflyVertex vertex, Object id, String k, V v) {
        final Key key = new Key(namespace, VERTEX_PROPERTY_AERO_SET, (Long) id);
        final Record r = read(key);
        Map<String, V> data;
        if (r == null)
            data = new HashMap<>();
        else
            data = (Map<String, V>) Optional.ofNullable(r.getMap(KEY_VALUE)).orElse(new HashMap<>());
        data.put(k, v);
        final Bin bin = new Bin(KEY_VALUE, Value.get(data));
        write(key, bin);

    }

    /**
     * Write a new Key : List[VertexProperty] on the associated vertex
     *
     * @param vertex
     * @param k
     * @param v
     */

    public void writeVertexPropertyList(final FireflyVertex vertex, final String k, final List<VertexProperty> v) {
        final Key vertexKey = new Key(namespace, VERTEX_AERO_SET, (Long) vertex.id());
        final Record vertexRecord = read(vertexKey);
        if (vertexRecord == null)
            throw new NoSuchElementException();
        Map<String, List<Object>> propertyKeys = (Map<String, List<Object>>) vertexRecord.getMap(VERTEX_PROPERTY_NAME_TO_ID);
        if (propertyKeys == null)
            propertyKeys = new HashMap<>();
        final List<Object> ids = propertyKeys.getOrDefault(k, new ArrayList<>());

        v.forEach(vp -> {
            writeVertexProperty(vertex, vp.id(), vp.key(), vp.value());
            ids.add(vp.id());
        });
        final HashSet<Object> uniqueIds = new HashSet<>(ids);

        propertyKeys.put(k, Arrays.asList(uniqueIds.toArray()));

        final Bin vertexPropertyIds = new Bin(VERTEX_PROPERTY_NAME_TO_ID, Value.get(propertyKeys));
        write(vertexKey, vertexPropertyIds);
    }

    /**
     * Remove a VertexProperty Record from the database
     *
     * @param graph
     * @param id
     */
    public void removeVertexProperty(FireflyGraph graph, Object id) {
        final Key key = new Key(namespace, VERTEX_PROPERTY_AERO_SET, (Long) id);
        removeElementId(FireflyVertexProperty.class, id);
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
            Property<V> prop = new FireflyProperty<>(ele, key1, value);
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
        final Key key = new Key(namespace, PROPERTY_AERO_SET, (Long) ele.id());
        final Record r = read(key);
        if (r == null) {
            throw new NoSuchElementException();
        }
        Object val = r.getMap(ELEMENT_PROPERTIES).get(k);
        if (val == null) {
            throw new NoSuchElementException();
        }

        return new FireflyProperty(ele, k, val);
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
        final Key key = new Key(namespace, PROPERTY_AERO_SET, (Long) element.id());
        final Record r = read(key);
        Map<String, Object> data;
        if (r == null)
            data = new HashMap<>();
        else
            data = (Map<String, Object>) Optional.ofNullable(r.getMap(ELEMENT_PROPERTIES)).orElse(new HashMap<>());
        data.put(k, value);
        final Bin bin = new Bin(ELEMENT_PROPERTIES, Value.get(data));
        write(key, bin);
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
        final Key key = new Key(namespace, PROPERTY_AERO_SET, (Long) element.id());
        final Record r = read(key);
        Map<String, Object> data;
        if (r == null)
            return;
        else
            data = (Map<String, Object>) Optional.ofNullable(r.getMap(ELEMENT_PROPERTIES)).orElse(new HashMap<>());
        if (!data.containsKey(k))
            return;
        else
            data.remove(k);
        final Bin bin = new Bin(ELEMENT_PROPERTIES, Value.get(data));
        write(key, bin);
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
        final Bin lbin = new Bin("label", Value.get(label));
        write(key, lbin);
    }

    /**
     * remove a Vertex Record
     *
     * @param graph
     * @param id
     */
    public void removeVertex(FireflyGraph graph, Object id) {
        final Key key = new Key(namespace, VERTEX_AERO_SET, (Long) id);
        removeElementId(FireflyVertex.class, id);
        delete(key);
    }

    /**
     * return the inbound edges for a FireflyVertex
     *
     * @param v
     * @return
     */
    public List<Object> getInEdgeIdsFromVertex(final FireflyVertex v) {
        final Key key = new Key(namespace, VERTEX_EDGELIST_AERO_SET, String.format("%s%d", Direction.IN.name(), (Long) v.id()));
        final Record r = read(key);
        if (r == null) {
            return new ArrayList<>();
        }
        Map<String, List<Long>> labelEdges = (Map<String, List<Long>>) r.getMap(LABEL_EDGES);
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        return labelEdges.entrySet().stream().flatMap(e -> e.getValue().stream()).collect(Collectors.toList());
    }

    /**
     * return the outbound edges for a FireflyVertex
     *
     * @param v
     * @return
     */
    public List<Object> getOutEdgeIdsFromVertex(final FireflyVertex v) {
        final Key key = new Key(namespace, VERTEX_EDGELIST_AERO_SET, String.format("%s%d", Direction.OUT.name(), (Long) v.id()));
        final Record r = read(key);
        if (r == null) {
            return new ArrayList<>();
        }
        Map<String, List<Long>> labelEdges = (Map<String, List<Long>>) r.getMap(LABEL_EDGES);
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        return labelEdges.entrySet().stream().flatMap(e -> e.getValue().stream()).collect(Collectors.toList());
    }

    /**
     * update the list of currently valid ids
     *
     * @param type
     * @param id
     */
    public void writeElementId(final Class<? extends FireflyElement> type, final Object id) {
        final id_config cfg = new id_config(type);
        final Key key = new Key(namespace, cfg.getAeroSet(), cfg.getIdKey());
        client.operate(client.writePolicyDefault, key,
                ListOperation.append(cfg.getIdBin(), Value.get(((Number) id).longValue())));
    }

    /**
     * update the list of currently valid ids
     *
     * @param type
     * @param id
     */
    public void removeElementId(final Class<? extends FireflyElement> type, final Object id) {
        final id_config cfg = new id_config(type);
        final Key key = new Key(namespace, cfg.getAeroSet(), cfg.getIdKey());
        client.operate(client.writePolicyDefault, key,
                ListOperation.removeByValue(cfg.getIdBin(),
                        Value.get(((Number) id).longValue()), ListReturnType.NONE));
    }

    /**
     * get a list of currently valid ids
     *
     * @param type
     * @return
     */
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

    /**
     * Add an edge to a vertex
     * each vertex contains a list named for the edge direction
     *
     * @param graph
     * @param vertexId
     * @param edge
     * @param direction
     */
    public void addEdgeToVertex(final FireflyGraph graph, final Object vertexId, final FireflyEdge edge, final Direction direction) {
        final Key key = new Key(namespace, VERTEX_EDGELIST_AERO_SET, String.format("%s%d", direction.name(), (Long) vertexId));
        final Record r = read(key);
        Map<String, List<Long>> labelEdges;
        if (r == null) {
            labelEdges = new HashMap<>();
        } else {
            labelEdges = (Map<String, List<Long>>) Optional.ofNullable(r.getMap(LABEL_EDGES)).orElse(new HashMap<>());
        }
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        List<Long> edges = labelEdges.getOrDefault(edge.label(), new ArrayList<>());
        edges.add(((Number) edge.id()).longValue());
        labelEdges.put(edge.label(), edges);
        final Bin deb = new Bin(LABEL_EDGES, Value.get(labelEdges));
        write(key, deb);
    }

    /**
     * remove an edge from a vertex
     * each vertex contains a list named for the edge direction
     *
     * @param graph
     * @param vertexId
     * @param edge
     * @param direction
     */
    public void removeEdgeFromVertex(final FireflyGraph graph, final Object vertexId, final FireflyEdge edge, final Direction direction) {
        final Key key = new Key(namespace, VERTEX_EDGELIST_AERO_SET, String.format("%s%d", direction.name(), (Long) vertexId));
        final Record r = read(key);
        if (r == null)
            return;
        Map<String, List<Long>> labelEdges = (Map<String, List<Long>>) r.getMap(LABEL_EDGES);
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        List<Long> edges = labelEdges.getOrDefault(edge.label(), new ArrayList<>());
        edges.remove(((Number) edge.id()).longValue());
        labelEdges.put(edge.label(), edges);
        final Bin deb = new Bin(LABEL_EDGES, Value.get(labelEdges));
        write(key, deb);
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
        final Bin lbin = new Bin("label", Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(inVertex.id()));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(outVertex.id()));
        write(key, lbin, inVbin, outVBin);
        FireflyEdge edge = readEdge(graph, id);
        addEdgeToVertex(graph, inVertex.id(), edge, Direction.IN);
        addEdgeToVertex(graph, outVertex.id(), edge, Direction.OUT);
        Iterator<Object> propIter = Arrays.stream(keyValues).iterator();
        while (propIter.hasNext()) {
            Object propKey = propIter.next();
            Object propVal = propIter.next();
            writeProperty(edge, (String) propKey, propVal);
        }
    }

    public void removeEdge(final FireflyGraph graph, final Object id) {
        final Key key = new Key(namespace, EDGE_AERO_SET, (Long) id);
        removeElementId(FireflyEdge.class, id);
        delete(key);
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


    public void dropDatabase() {
        client.truncate(null, namespace, EDGE_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, PROPERTY_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_PROPERTY_AERO_SET, Calendar.getInstance());
        client.truncate(null, namespace, ID_MANAGER_SET, Calendar.getInstance());
        client.truncate(null, namespace, TEST_SET, Calendar.getInstance());
        client.truncate(null, namespace, VERTEX_EDGELIST_AERO_SET, Calendar.getInstance());
    }

    @Override
    public final String toString() {
        return String.format("%s %s %s", host, port, namespace);
    }

    public void close() {
        this.client.close();
    }

}
