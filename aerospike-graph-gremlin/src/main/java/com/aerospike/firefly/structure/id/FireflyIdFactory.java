package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdFactory {
    private final AerospikeConnection db;

    // Map from classes we support as TinkerPop ids to on disk type hints. NOTE: String not fully supported yet.
    private enum IdType {
        Null, Long, Integer, Double, Byte, String
    }

    private static final Map<Class<? extends Serializable>, Long> TYPE_TO_HINT = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(byte[].class, 4L);
        put(String.class, 5L);
    }};
    static final Map<Long, Class<? extends Serializable>> HINT_TO_TYPE = new HashMap<>() {{
        put(null, null);
        put(0L, null);
        put(1L, Long.class);
        put(2L, Integer.class);
        put(3L, Double.class);
        put(4L, byte[].class);
        put(5L, String.class);
    }};

    private FireflyIdFactory(final AerospikeConnection db) {
        this.db = db;
    }

    /**
     * Create a FireflyIdFactory
     *
     * @param db the AerospikeConnection
     * @return FireflyIdFactory
     */
    public static FireflyIdFactory create(final AerospikeConnection db) {
        return new FireflyIdFactory(db);
    }

    /**
     * Create an id for a specific FireflyElement type.
     *
     * @param type Type to create element for.
     * @param id   Id to use for element.
     * @return FireflyId.
     */
    public FireflyId createFromUser(final Class<? extends FireflyElement> type, Object id) {
        if (id instanceof FireflyElement) {
            return ((FireflyElement) id).id;
        } else if (id instanceof Element) {
            id = ((Element) id).id();
        }

        if (id instanceof Float) {
            id = ((Float) id).doubleValue();
        }
        boolean supportedType = TYPE_TO_HINT.containsKey(id.getClass());
        Object tempId;
        if (id instanceof String) {
            try {
                tempId = Long.parseLong((String) id);
            } catch (NumberFormatException e) {
                tempId = id;
            }
        } else {
            tempId = id;
        }
        if (!FireflyVertex.class.isAssignableFrom(type) &&
                !FireflyEdge.class.isAssignableFrom(type) &&
                !FireflyVertexProperty.class.isAssignableFrom(type)) {
            throw new UnsupportedOperationException(type + " not a Firefly Element ");
        }

        if (FireflyVertex.class.isAssignableFrom(type)) {
            if (!supportedType)
                throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return createId(tempId, TYPE_TO_HINT.get(id.getClass()), FireflyVertex.class);
        } else if (FireflyEdge.class.isAssignableFrom(type)) {
            if (!supportedType)
                throw Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return createId(tempId, TYPE_TO_HINT.get(id.getClass()), FireflyEdge.class);
        } else if (FireflyVertexProperty.class.isAssignableFrom(type)) {
            if (!supportedType)
                throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return createId(tempId, TYPE_TO_HINT.get(id.getClass()), FireflyVertexProperty.class);
        } else {
            throw new UnsupportedOperationException(type + " not a Firefly Element.");
        }
    }

    /**
     * Create an id using the id typeHint type and id object. If typeHint is null, it is not required.
     *
     * @param id  Id Object.
     * @param typeHint typeHint to use, null if not user defined.
     * @return FireflyId.
     */
    private FireflyId createId(final Object id, final Long typeHint, final Class<? extends FireflyElement> type) {
        final String set = db.setFromElementType(type);
        if (!HINT_TO_TYPE.containsKey(typeHint)) {
            // This is just caught and propagated up via a gremlin specific exception.
            throw new IllegalArgumentException("Invalid id type: " + typeHint + ". Id type must be one of " + HINT_TO_TYPE.keySet());
        } else if (FireflyEdge.class.isAssignableFrom(type)) {
            if (id instanceof ByteBuffer) {
                return new FireflyPhatEdgeId((ByteBuffer) id, db.PHAT_EDGE_SIZE, set);
            } else if (byte[].class.isAssignableFrom(id.getClass())) {
                return new FireflyPhatEdgeId(ByteBuffer.wrap((byte[]) id), db.PHAT_EDGE_SIZE, set);
            } else if (id instanceof String) {
                try {
                    byte[] decodedBytes = Base64.getDecoder().decode((String) id);
                    if (decodedBytes.length != 16) {
                        throw new IllegalArgumentException("Invalid id for edge: '" + id + "'. Base64 encoded String did not decode to a valid 16 byte array.");
                    }
                    return new FireflyPhatEdgeId(ByteBuffer.wrap(decodedBytes), db.PHAT_EDGE_SIZE, set);
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException("Invalid id for edge: '" + id + "'. Id type must be ByteBuffer, byte[], or base64 encoded String.");
                }
            } else {
                throw new IllegalArgumentException("Invalid id for edge: '" + id + "'. Id type must be ByteBuffer, byte[], or base64 encoded String.");
            }
        } else if (Number.class.isAssignableFrom(id.getClass())) {
            return FireflyIdPoly.fromObject(id, HINT_TO_TYPE.get(typeHint), set);
        } else if (String.class.isAssignableFrom(id.getClass())) {
            try {
                // For numeric string ids.
                final Number numericId = Long.parseLong((String) id);
                return FireflyIdPoly.fromObject(numericId, HINT_TO_TYPE.get(typeHint), set);
            } catch (NumberFormatException ignored) {
                return FireflyIdPoly.fromObject((String) id, set);
            }
        } else if (byte[].class.isAssignableFrom(id.getClass())) {
            return new FireflyIdComposite(db, (byte[]) id);
        }
        throw new IllegalArgumentException("Invalid id type: " + id.getClass() + ". Id type must be one of " + TYPE_TO_HINT.keySet());
    }

    /**
     * Create an id using the id hint type and id object. If idx is null, it is not required.
     *
     * @param id Id Object.
     * @return FireflyId.
     */
    public FireflyId createId(final Object id, final Class<? extends FireflyElement> type) {
        Object idObj = id;
        if (id instanceof Element) {
            idObj = ((Element) id).id();
        } else if (id instanceof FireflyId) {
            return (FireflyId) id;
        }
        return createId(idObj, TYPE_TO_HINT.get(id.getClass()), type);
    }

    /**
     * Generate a new id from the id manager.
     * @param graph the FireflyGraph instance
     * @param type the Firefly Element Type to generate an id for
     * @return FireflyId
     */
    public FireflyId createFromManager(final FireflyGraph graph, final Class<? extends FireflyElement> type) {
        if (FireflyVertex.class.isAssignableFrom(type)) {
            return createId(graph.vertexIdManager.getNextId(graph), type);
        } else if (FireflyEdge.class.isAssignableFrom(type)) {
            return createId(graph.edgeIdManager.getNextId(graph), type);
        } else if (FireflyVertexProperty.class.isAssignableFrom(type)) {
            return createId(graph.vertexPropertyIdManager.getNextId(graph), type);
        } else {
            throw new UnsupportedOperationException(type + " not a Firefly Element ");
        }
    }

    /**
     * Create a composite id from an edge and a vertex id
     * @param edgeId the edge id
     * @param adjacentVertex the adjacent vertex id
     * @return a FireflyIdComposite representing an edge and an adjacent Vertex
     */
    public FireflyId createCompositeEdgeId(final FireflyId edgeId, final FireflyId adjacentVertex) {
        return new FireflyIdComposite(db, edgeId, adjacentVertex);
    }

    public FireflyId createFromKeyValues(final Class<? extends FireflyElement> type, final Object... keyValues) {
        final Optional<Object> id = ElementHelper.getIdValue(keyValues);
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Id not found in keyValues");
        } else {
            return createFromUser(type, id.get());
        }
    }

    /**
     * Create a FireflyId from an Aerospike Record and Firefly Element class
     * @param db the AerospikeConnection
     * @param record the Record representing the Firefly Element
     * @param type the type of Firefly Element to create
     * @return FireflyId
     */
    public FireflyId createFromRecord(final AerospikeConnection db, final FireflyRecord record, final Class<? extends FireflyElement> type) {
        //@todo uses userKey, check if this works when key is constructed from hash
        final Object origId;
        final long typeHint;
        if (record.key().userKey.getObject() != null) {
            origId = record.key().userKey.getObject();
        } else if (record.record().getValue(db.USER_KEY_BIN) != null) {
            origId = record.record().getValue(db.USER_KEY_BIN);
        } else { //@todo list of cases
            throw new RuntimeException("no key available"); //maybe a pure hash id
        }
        typeHint = record.record().getLong(db.ID_TYPE_BIN) == 0 ? FireflyIdPoly.STORAGE_TYPE_HINTS.get(origId.getClass()) : record.record().getLong(db.ID_TYPE_BIN);
        return createId(origId, typeHint, type);
    }

    public Map<String, List<FireflyId>> convertMapListObjectToFireflyIdMap(final Map<String, List<Object>> fireflyObjectIds) {
        if (fireflyObjectIds == null) {
            return new HashMap<>();
        }
        final Map<String, List<FireflyId>> labelEdgeIds = new HashMap<>();
        for (final String label : fireflyObjectIds.keySet()) {
            final List<FireflyId> fireflyIds = new ArrayList<>();
            for (final Object edge : fireflyObjectIds.get(label)) {
                if (edge instanceof byte[]) {
                    fireflyIds.add(new FireflyIdComposite(db, (byte[]) edge));
                } else {
                    fireflyIds.add(createId(edge, FireflyEdge.class));
                }
            }
            labelEdgeIds.put(label, fireflyIds);
        }
        return labelEdgeIds;
    }

    public Map<String, Object> convertMapToStorage(final Map<String, FireflyId> fireflyObjectIds) {
        if (fireflyObjectIds == null) {
            return new TreeMap<>();
        }
        final Map<String, Object> labelEdgeIds = new TreeMap<>();
        for (final String label : fireflyObjectIds.keySet()) {
            final Object id = fireflyObjectIds.get(label).getStorageId();
            labelEdgeIds.put(label, id);
        }
        return labelEdgeIds;
    }

    public Map<String, FireflyId> convertMapObjectToFireflyIdMap(final Map<String, Object> fireflyObjectIds, final Class<? extends FireflyElement> type) {
        if (fireflyObjectIds == null) {
            return new TreeMap<>();
        }
        final Map<String, FireflyId> edgeIdMap = new TreeMap<>();
        for (final String label : fireflyObjectIds.keySet()) {
            edgeIdMap.put(label, createId(fireflyObjectIds.get(label), type));
        }
        return edgeIdMap;
    }
}
