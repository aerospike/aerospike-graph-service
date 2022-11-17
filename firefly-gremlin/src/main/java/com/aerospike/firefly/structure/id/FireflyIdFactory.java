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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdFactory {
    // Map from classes we support as TinkerPop ids to on disk type hints. NOTE: String not fully supported yet.
    private static final Map<Class<? extends Serializable>, Long> TYPE_TO_IDX = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(Double.class, 3L);
        put(byte[].class, 4L);
        put(String.class, 5L);
    }};
    private static final Map<Long, Class<? extends Serializable>> IDX_TO_TYPE = new HashMap<>() {{
        put(null, null);
        put(0L, null);
        put(1L, Long.class);
        put(2L, Integer.class);
        put(3L, Double.class);
        put(4L, byte[].class);
        put(5L, String.class);
    }};

    /**
     * Create an id for a specific FireflyElement type.
     *
     * @param id          Id to use for element.
     * @param adjacentVertex  Id of adjacent vertex.
     * @return FireflyId.
     */
    public static FireflyId createEdgeIdFromUser(final Object id, final FireflyId adjacentVertex) {
        // Generate edge id.
        final FireflyId edgeId = createFromUser(FireflyEdge.class, id);

        // Generate composite id.
        return new FireflyIdComposite(edgeId, adjacentVertex);
    }

    /**
     * Create an id for a specific FireflyElement type.
     *
     * @param type Type to create element for.
     * @param id   Id to use for element.
     * @return FireflyId.
     */
    public static FireflyId createFromUser(final Class<? extends FireflyElement> type, Object id) {
        if (id instanceof FireflyElement) {
            return ((FireflyElement) id).id;
        } else if (id instanceof Element) {
            id = ((Element) id).id();
        }

        if (id instanceof Float) {
            id = ((Float) id).doubleValue();
        }
        boolean supportedType = TYPE_TO_IDX.containsKey(id.getClass());
        final Object numericId;
        if (id instanceof String) {
            try {
                numericId = Long.parseLong((String) id);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("String id must be a number.");
            }
        } else {
            numericId = id;
        }

        if (!FireflyVertex.class.isAssignableFrom(type) &&
                !FireflyEdge.class.isAssignableFrom(type) &&
                !FireflyVertexProperty.class.isAssignableFrom(type)) {
            throw new UnsupportedOperationException(type + " not a Firefly Element ");
        } else if (FireflyVertex.class.isAssignableFrom(type) && !supportedType) {
            System.out.println("Type: " + type);
            System.out.println("Id: " + id);
            System.out.println("Id.class(): " + id.getClass().getName());
            throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        } else if (FireflyEdge.class.isAssignableFrom(type) && !supportedType) {
            throw Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        } else if (FireflyVertexProperty.class.isAssignableFrom(type) && !supportedType) {
            throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        }

        // Does element with this id already exist validation is done in the element requesting the id.
        return createId(numericId, TYPE_TO_IDX.get(id.getClass()));
    }

    /**
     * Create an id using the id idx type and id object. If idx is null, it is not required.
     *
     * @param id  Id Object.
     * @param idx Idx to use, null if not user defined.
     * @return FireflyId.
     */
    private static FireflyId createId(final Object id, final Long idx) {
        if (!IDX_TO_TYPE.containsKey(idx)) {
            // This is just caught and propagated up via a gremlin specific exception.
            throw new IllegalArgumentException("Invalid id type: " + idx + ". Id type must be one of " + IDX_TO_TYPE.keySet());
        }
        if (Number.class.isAssignableFrom(id.getClass())) {
            return new FireflyIdNumeric((Number) id, IDX_TO_TYPE.get(idx));
        } else if (String.class.isAssignableFrom(id.getClass())) {
            try {
                // For numeric string ids.
                final Number numericId = Long.parseLong((String) id);
                return new FireflyIdNumeric(numericId, IDX_TO_TYPE.get(idx));
            } catch (NumberFormatException ignored) {
                return new FireflyIdString((String) id);
            }
        } else if (byte[].class.isAssignableFrom(id.getClass())) {
            return new FireflyIdComposite((byte[]) id);
        }
        throw new IllegalArgumentException("Invalid id type: " + id.getClass() + ". Id type must be one of " + TYPE_TO_IDX.keySet());
    }

    /**
     * Create an id using the id idx type and id object. If idx is null, it is not required.
     *
     * @param id Id Object.
     * @return FireflyId.
     */
    public static FireflyId createId(final Object id) {
        Object idObj = id;
        if (id instanceof Element) {
            idObj = ((Element) id).id();
        } else if (id instanceof FireflyId) {
            return (FireflyId) id;
        }
        return createId(idObj, null);
    }

    public static FireflyId createFromManager(final FireflyGraph graph, final Class<? extends FireflyElement> type) {
        if (FireflyVertex.class.isAssignableFrom(type)) {
            return createId(graph.vertexIdManager.getNextId(graph), null);
        } else if (FireflyEdge.class.isAssignableFrom(type)) {
            return createId(graph.edgeIdManager.getNextId(graph), null);
        } else if (FireflyVertexProperty.class.isAssignableFrom(type)) {
            return createId(graph.vertexPropertyIdManager.getNextId(graph), null);
        } else {
            throw new UnsupportedOperationException(type + " not a Firefly Element ");
        }
    }

    public static FireflyId createEdgeId(final FireflyId edgeId, final FireflyId adjacentVertex) {
        return new FireflyIdComposite(edgeId, adjacentVertex);
    }

    public static FireflyId createEdgeIdFromManager(final FireflyGraph graph, final FireflyId adjacentVertex) {
        return new FireflyIdComposite(createId(graph.edgeIdManager.getNextId(graph), null), adjacentVertex);
    }

    public static FireflyId createFromKeyValues(final Class<? extends FireflyElement> type, final Object... keyValues) {
        final Optional<Object> id = ElementHelper.getIdValue(keyValues);
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Id not found in keyValues");
        } else {
            return createFromUser(type, id.get());
        }
    }

    public static FireflyId createEdgeIdFromKeyValues(final FireflyId adjacentVertexId, final Object... keyValues) {
        final Optional<Object> id = ElementHelper.getIdValue(keyValues);
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Id not found in keyValues");
        } else {
            return createEdgeIdFromUser(id.get(), adjacentVertexId);
        }
    }

    public static FireflyId createFromRecord(final AerospikeConnection db, final FireflyRecord record) {
        return createId(record.key().userKey.getObject(), record.record.getLong(db.ID_TYPE));
    }

    public static Map<String, List<FireflyId>> convertMapListObjectToFireflyIdMap(final Map<String, List<Object>> fireflyObjectIds) {
        if (fireflyObjectIds == null) {
            return new HashMap<>();
        }
        final Map<String, List<FireflyId>> labelEdgeIds = new HashMap<>();
        for (final String label : fireflyObjectIds.keySet()) {
            final List<FireflyId> fireflyIds = new ArrayList<>();
            for (final Object edge : fireflyObjectIds.get(label)) {
                if (edge instanceof byte[]) {
                    fireflyIds.add(new FireflyIdComposite((byte[]) edge));
                } else {
                    fireflyIds.add(FireflyIdFactory.createId(edge));
                }
            }
            labelEdgeIds.put(label, fireflyIds);
        }
        return labelEdgeIds;
    }

    public static Map<String, List<Object>> convertMapListToStorage(final Map<String, List<FireflyId>> fireflyObjectIds) {
        if (fireflyObjectIds == null) {
            return new HashMap<>();
        }

        final Map<String, List<Object>> labelEdgeIds = new HashMap<>();
        for (final String label : fireflyObjectIds.keySet()) {
            final List<Object> ids = new ArrayList<>();
            for (final FireflyId id : fireflyObjectIds.get(label)) {
                ids.add(id.getStorageId());
            }
            labelEdgeIds.put(label, ids);
        }
        return labelEdgeIds;
    }

    public static Map<String, List<Object>> convertMapListToCache(final Map<String, List<FireflyId>> fireflyObjectIds) {
        if (fireflyObjectIds == null) {
            return new HashMap<>();
        }
        final Map<String, List<Object>> labelEdgeIds = new HashMap<>();
        for (final String label : fireflyObjectIds.keySet()) {
            final List<Object> ids = new ArrayList<>();
            for (final FireflyId id : fireflyObjectIds.get(label)) {
                ids.add(id.getCachedId());
            }
            labelEdgeIds.put(label, ids);
        }
        return labelEdgeIds;
    }

    public static Map<String, Object> convertMapToStorage(final Map<String, FireflyId> fireflyObjectIds) {
        if (fireflyObjectIds == null) {
            return new HashMap<>();
        }
        final Map<String, Object> labelEdgeIds = new HashMap<>();
        for (final String label : fireflyObjectIds.keySet()) {
            final Object id = fireflyObjectIds.get(label).getStorageId();
            labelEdgeIds.put(label, id);
        }
        return labelEdgeIds;
    }

    public static Map<String, Object> convertMapToCache(final Map<String, FireflyId> fireflyObjectIds) {
        if (fireflyObjectIds == null) {
            return new HashMap<>();
        }
        final Map<String, Object> labelEdgeIds = new HashMap<>();
        for (final String label : fireflyObjectIds.keySet()) {
            final Object id = fireflyObjectIds.get(label).getCachedId();
            labelEdgeIds.put(label, id);
        }
        return labelEdgeIds;
    }

    public static Map<String, FireflyId> convertMapObjectToFireflyIdMap(final Map<String, Object> fireflyObjectIds) {
        if (fireflyObjectIds == null) {
            return new HashMap<>();
        }
        final Map<String, FireflyId> edgeIdMap = new HashMap<>();
        for (final String label : fireflyObjectIds.keySet()) {
            edgeIdMap.put(label, FireflyIdFactory.createId(fireflyObjectIds.get(label)));
        }
        return edgeIdMap;
    }

    public static List<FireflyId> convertObjectListToFireflyIdList(final List<Object> fireflyObjectIds) {
        if (fireflyObjectIds == null) {
            return new ArrayList<>();
        }
        List<FireflyId> fireflyIds = new ArrayList<>();
        for (final Object edge : fireflyObjectIds) {
            fireflyIds.add(FireflyIdFactory.createId(edge));
        }
        return fireflyIds;
    }
}
