package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.aerospike.firefly.util.Tokens.EDGE_UNIQUE_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.EDGE_PACKING_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.VERTEX_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.VERTEX_PROPERTY_ID_COUNTER;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdFactory {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyIdFactory.class);

    private final AerospikeConnection db;
    private final IdManager<Long> vertexIdManager;
    private final IdManager<byte[]> edgeIdManager;
    private final IdManager<Long> vertexPropertyIdManager;

    public static final Map<Class<? extends Serializable>, Long> VERTEX_ID_TYPE_TO_HINT = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(String.class, 5L);
    }};

    private static final Map<Long, Class<? extends Serializable>> HINT_TO_TYPE = VERTEX_ID_TYPE_TO_HINT.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    public FireflyIdFactory(final AerospikeConnection db) {
        this.db = db;
        this.vertexIdManager = new BufferedNumericIdManager(VERTEX_ID_COUNTER, db.VERTEX_ID_BUFFER_SIZE);
        if (db.MRT_ENABLED && !db.getBulkLoaderFlag()) {
            this.edgeIdManager = new MrtRecyclingBufferedNumericIdManager(EDGE_UNIQUE_ID_COUNTER, EDGE_PACKING_ID_COUNTER, db.EDGE_ID_BUFFER_SIZE, db.EDGE_ID_RECYCLE_BUFFER_SIZE, db.PHAT_EDGE_SIZE);
        } else {
            this.edgeIdManager = new RecyclingBufferedNumericIdManager(EDGE_UNIQUE_ID_COUNTER, EDGE_PACKING_ID_COUNTER, db.EDGE_ID_BUFFER_SIZE, db.EDGE_ID_RECYCLE_BUFFER_SIZE);
        }
        this.vertexPropertyIdManager = new BufferedNumericIdManager(VERTEX_PROPERTY_ID_COUNTER, db.PROPERTY_ID_BUFFER_SIZE);
    }

    public FireflyId createVertexId(final Object id) {
        Object convertedId = id;
        if (convertedId instanceof FireflyVertex) {
            return ((FireflyElement) id).id;
        } else if (id instanceof Element) {
            convertedId = ((Element) id).id();
        }

        if (convertedId instanceof String) {
            try {
                // Tinkerpop treats Strings that are Long-parsable as a Long, but still need to retain their status as a String.
                final long stringIdAsLong = Long.parseLong((String) convertedId);
                return FireflyIdPoly.fromObject(stringIdAsLong, convertedId.getClass(), this.db.VERTEX_AERO_SET);
            } catch (final NumberFormatException e) {
                // Do nothing - ID is a String
            }
        }

        final Class idClass = convertedId.getClass();
        if (!VERTEX_ID_TYPE_TO_HINT.containsKey(idClass)) {
            LOG.error("Invalid id type for vertex: {}.", idClass);
            throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        }

        return FireflyIdPoly.fromObject(convertedId, this.db.VERTEX_AERO_SET);
    }

    /**
     * Create a FireflyId for a Vertex from a record. This is used to maintain the original user id type after being
     * written and read back from Aerospike.
     *
     * @param record the Vertex record
     * @return The FireflyId for the Vertex with a persisted user id type hint.
     */
    public FireflyId createVertexIdFromRecord(final FireflyRecord record) {
        final Object userId;
        final long typeHint;
        if (record.key().userKey.getObject() != null) {
            userId = record.key().userKey.getObject();
        } else if (record.record().getValue(db.USER_KEY_BIN) != null) {
            userId = record.record().getValue(db.USER_KEY_BIN);
        } else {
            // This should never happen since Vertex records should always have user id stored.
            throw new RuntimeException("Vertex record did not contain a user key.");
        }
        typeHint = record.record().getLong(db.ID_TYPE_BIN) == 0 ? VERTEX_ID_TYPE_TO_HINT.get(userId.getClass()) : record.record().getLong(db.ID_TYPE_BIN);
        if (HINT_TO_TYPE.containsKey(typeHint)) {
            return FireflyIdPoly.fromObject(userId, HINT_TO_TYPE.get(typeHint), db.setFromElementType(FireflyVertex.class));
        } else {
            // This should never happen.
            throw new RuntimeException("Vertex record contained an unexpected user id type hint: " + typeHint);
        }
    }

    public FireflyId createVertexIdFromHash(final String hash) {
        return FireflyIdPoly.fromHashString(hash, db.VERTEX_AERO_SET);
    }

    public FireflyId createVertexIdFromHash(final byte[] hash) {
        return FireflyIdPoly.fromHash(hash, db.VERTEX_AERO_SET);
    }

    public FireflyPhatEdgeId createEdgeId(final Object id) {
        if (id instanceof ByteBuffer) {
            return FireflyPhatEdgeId.fromByteBuffer((ByteBuffer) id, db.PHAT_EDGE_SIZE, db.EDGE_AERO_SET);
        } else if (byte[].class.isAssignableFrom(id.getClass())) {
            return FireflyPhatEdgeId.fromByteArray((byte[]) id, db.PHAT_EDGE_SIZE, db.EDGE_AERO_SET);
        } else if (id instanceof String) {
            return FireflyPhatEdgeId.fromBase64String((String) id, db.PHAT_EDGE_SIZE, db.EDGE_AERO_SET);
        } else {
            throw new IllegalArgumentException("Invalid id for edge: '" + id + "'. Id type must be ByteBuffer, byte[], or base64 encoded String.");
        }
    }

    public FireflyId createVertexPropertyId(final Object id) {
        if (id instanceof LazyIdTransform) {
            return ((LazyIdTransform) id).transform();
        }
        if (!(id instanceof Number)) {
            throw new IllegalArgumentException("Vertex Property ID must be a valid Long type.");
        }
        final String set = db.setFromElementType(FireflyVertexProperty.class);
        return FireflyIdPoly.fromObject(((Number) id).longValue(), set);
    }

    public FireflyId createGraphVariableId(final Object id) {
        final String set = db.GRAPH_VARIABLES_SET;
        return FireflyIdPoly.fromObject(id, set);
    }

    /**
     * Create a composite id from an edge and a vertex id
     * @param edgeId            the edge id
     * @param adjacentVertex    the adjacent vertex id
     * @return a FireflyIdComposite representing an edge and an adjacent Vertex
     */
    public FireflyIdComposite createCompositeEdgeId(final FireflyEdgeId edgeId, final FireflyId adjacentVertex) {
        return new FireflyIdComposite(db, edgeId, adjacentVertex);
    }

    /**
     * Create a composite id from a List
     * @param compositeIdArray  the List that forms a FireflyIdComposite
     * @return a FireflyIdComposite representing an edge and an adjacent Vertex
     */
    public FireflyIdComposite createCompositeEdgeId(final List<Object> compositeIdArray) {
        return new FireflyIdComposite(db, compositeIdArray);
    }

    /**
     * Generate a new id for a FireflyElement.
     *
     * @param graph the FireflyGraph to which the new id belongs
     * @param type  the FireflyElement type to generate an id for
     * @return the newly generated FireflyId
     */
    public FireflyId generateId(final FireflyGraph graph, final Class<? extends FireflyElement> type) {
        if (FireflyVertex.class.isAssignableFrom(type)) {
            return createVertexId(this.vertexIdManager.getNextId(graph));
        } else if (FireflyEdge.class.isAssignableFrom(type)) {
            return createEdgeId(this.edgeIdManager.getNextId(graph));
        } else if (FireflyVertexProperty.class.isAssignableFrom(type)) {
            return createVertexPropertyId(this.vertexPropertyIdManager.getNextId(graph));
        } else {
            // This should never happen.
            throw new IllegalArgumentException("Invalid FireflyElement type: " + type);
        }
    }

    public byte[] generateRawEdgeId(final FireflyGraph graph) {
        return this.edgeIdManager.getNextId(graph);
    }

    public void recycleEdgeId(final FireflyId id) {
        this.edgeIdManager.recycleId(id);
    }

    public void convertMapToLazyIdsInPlace(final Map<String, ?> fireflyObjectIds,
                                           final FireflyGraph graph,
                                           final Class<? extends LazyIdTransform> type) {
        // Code below complains without the supression and cast to <String, Object>.
        @SuppressWarnings("unchecked")
        final Map<String, Object> fireflyObjectIdsMap = (Map) fireflyObjectIds;
        if (fireflyObjectIdsMap == null) {
            return;
        }
        fireflyObjectIdsMap.forEach((key, value) -> {
            if (value instanceof List<?>) {
                final List<Object> list = (List<Object>) value;
                fireflyObjectIdsMap.replace(key,
                        list.stream().map(id -> {
                            if (id instanceof FireflyId) {
                                return new LazyIdTransform((FireflyId) id);
                            } else if (id instanceof LazyIdTransform) {
                                return id;
                            } else {
                                return LazyIdTransform.create(id, graph, type);
                            }
                        }).collect(Collectors.toList()));
            } else {
                if (value instanceof FireflyId) {
                    fireflyObjectIdsMap.replace(key, new LazyIdTransform((FireflyId) value));
                } else if (!(value instanceof LazyIdTransform)) {
                    fireflyObjectIdsMap.replace(key, LazyIdTransform.create(value, graph, type));
                }
            }
        });
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

    public long getTypeHint(final Object id) {
        return VERTEX_ID_TYPE_TO_HINT.get(id.getClass());
    }

    public long getTypeHint(final Class<?> objectClass) {
        return VERTEX_ID_TYPE_TO_HINT.get(objectClass);
    }

    // For testing purposes only
    public FireflyId getTestId(final Object id) {
        return FireflyIdPoly.fromObject(id, db.TEST_SET);
    }

    // For testing purposes only
    public IdManager<Long> getVertexIdManager() {
        return this.vertexIdManager;
    }

    // For testing purposes only
    public IdManager<byte[]> getEdgeIdManager() {
        return this.edgeIdManager;
    }
}
