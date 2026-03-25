package com.aerospike.firefly.structure.id;

import com.aerospike.client.query.KeyRecord;
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
    private final RecyclingEdgeIdManager edgeIdManager;
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
        this.vertexIdManager = new DecrementingNumericIdManager(VERTEX_ID_COUNTER, db.getConfig().vertexIdBufferSize);
        if (db.getConfig().mrtEnabled || db.getConfig().transactionEnabled) {
            this.edgeIdManager = new MrtRecyclingBufferedNumericIdManager(EDGE_PACKING_ID_COUNTER, EDGE_UNIQUE_ID_COUNTER,
                    db.getConfig().edgeIdBufferSize, db.getConfig().edgeIdRecycleBufferSize, db.getConfig().phatEdgeSize);
        } else {
            this.edgeIdManager = new RecyclingBufferedNumericIdManager(EDGE_PACKING_ID_COUNTER, EDGE_UNIQUE_ID_COUNTER,
                    db.getConfig().edgeIdBufferSize, db.getConfig().edgeIdRecycleBufferSize);
        }
        this.vertexPropertyIdManager = new DecrementingNumericIdManager(VERTEX_PROPERTY_ID_COUNTER, db.getConfig().propertyIdBufferSize);
    }

    public FireflyId createVertexId(final Object id) {
        Object convertedId = id;
        if (convertedId instanceof FireflyVertex) {
            return ((FireflyElement) id).id;
        } else if (id instanceof Element) {
            convertedId = ((Element) id).id();
        }

        if (convertedId instanceof String) {
            final String strId = (String) convertedId;
            if (isAllDigits(strId)) {
                final long stringIdAsLong = Long.parseLong(strId, 0, strId.length(), 10);
                return FireflyIdPoly.fromObject(stringIdAsLong, String.class, this.db.getConfig().vertexAeroSet);
            }
        }

        final Class<?> idClass = convertedId.getClass();
        if (!VERTEX_ID_TYPE_TO_HINT.containsKey(idClass)) {
            LOG.error("Invalid id type for vertex: {}.", idClass);
            throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        }

        return FireflyIdPoly.fromObject(convertedId, this.db.getConfig().vertexAeroSet);
    }

    private static boolean isAllDigits(String s) {
        int len = s.length();
        if (len == 0) return false;
        int i = (s.charAt(0) == '-') ? 1 : 0;
        for (; i < len; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        return true;
    }

    /**
     * Create a FireflyId for a Vertex from a record. This is used to maintain the original user id type after being
     * written and read back from Aerospike.
     *
     * @param record the Vertex record
     * @return The FireflyId for the Vertex with a persisted user id type hint.
     */
    public FireflyId createVertexIdFromRecord(final KeyRecord record) {
        final Object userId;
        if (record.record.getValue(db.getConfig().userKeyBin) != null) {
            userId = record.record.getValue(db.getConfig().userKeyBin);
        } else {
            // This should never happen since Vertex records should always have user id stored.
            throw new RuntimeException("Vertex record did not contain a user key.");
        }
        final long typeHint = record.record.getLong(db.getConfig().idTypeBin);
        if (HINT_TO_TYPE.containsKey(typeHint)) {
            return FireflyIdPoly.fromObject(userId, HINT_TO_TYPE.get(typeHint), db.setFromElementType(FireflyVertex.class));
        } else {
            // This should never happen.
            throw new RuntimeException("Vertex record contained an unexpected user id type hint: " + typeHint);
        }
    }

    public FireflyId createVertexIdFromHash(final String hash) {
        return FireflyIdPoly.fromHashString(hash, db.getConfig().vertexAeroSet);
    }

    public FireflyId createVertexIdFromHash(final byte[] hash) {
        return FireflyIdPoly.fromHash(hash, db.getConfig().vertexAeroSet);
    }

    public FireflyPhatEdgeId createEdgeId(final Object id) {
        if (id instanceof ByteBuffer) {
            return FireflyPhatEdgeId.fromByteBuffer((ByteBuffer) id, db.getConfig().phatEdgeSize, db.getConfig().edgeAeroSet);
        } else if (byte[].class.isAssignableFrom(id.getClass())) {
            return FireflyPhatEdgeId.fromByteArray((byte[]) id, db.getConfig().phatEdgeSize, db.getConfig().edgeAeroSet);
        } else if (id instanceof String) {
            return FireflyPhatEdgeId.fromBase64String((String) id, db.getConfig().phatEdgeSize, db.getConfig().edgeAeroSet);
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
        final String set = db.getConfig().graphVariablesSet;
        return FireflyIdPoly.fromObject(id, set);
    }

    /**
     * Create a composite id from an edge and a vertex id
     *
     * @param edgeId         the edge id
     * @param adjacentVertex the adjacent vertex id
     * @return a FireflyIdComposite representing an edge and an adjacent Vertex
     */
    public FireflyIdComposite createCompositeEdgeId(final FireflyEdgeId edgeId, final FireflyId adjacentVertex) {
        return new FireflyIdComposite(db, edgeId, adjacentVertex);
    }

    /**
     * Create a composite id from a List
     *
     * @param compositeIdArray the List that forms a FireflyIdComposite
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

    public void recycleEdgeId(final FireflyId id, final FireflyGraph graph, final boolean wasIdCommitted) {
        this.edgeIdManager.recycleId(id, graph, wasIdCommitted);
    }

    public void recycleCurrentPack() {
        if (this.edgeIdManager instanceof MrtRecyclingBufferedNumericIdManager) {
            ((MrtRecyclingBufferedNumericIdManager) this.edgeIdManager).recycleCurrentPack();
        } else {
            // This should never happen.
            throw new IllegalStateException("Attempted to recycle Edge record pack ID. Please contact support.");
        }
    }

    public FireflyEdgeId generateNonRecycledEdgeId(final FireflyGraph graph) {
        return createEdgeId(this.edgeIdManager.getNewId(graph));
    }

    public void convertMapToLazyIdsInPlace(final Map<String, ?> fireflyObjectIds,
                                           final FireflyGraph graph,
                                           final Class<? extends LazyIdTransform> type) {
        // Code below complains without the suppression and cast to <String, Object>.
        @SuppressWarnings("unchecked") final Map<String, Object> fireflyObjectIdsMap = (Map<String, Object>) fireflyObjectIds;
        if (fireflyObjectIdsMap == null) {
            return;
        }
        fireflyObjectIdsMap.forEach((key, value) -> {
            if (value instanceof List<?>) {
                final List<Object> list = (List<Object>) value;
                for (int i = 0; i < list.size(); i++) {
                    final Object id = list.get(i);
                    if (id instanceof FireflyId) {
                        list.set(i, new LazyIdTransform((FireflyId) id));
                    } else if (!(id instanceof LazyIdTransform)) {
                        list.set(i, LazyIdTransform.create(id, graph, type));
                    }
                }
            } else {
                if (value instanceof FireflyId) {
                    fireflyObjectIdsMap.replace(key, new LazyIdTransform((FireflyId) value));
                } else if (!(value instanceof LazyIdTransform)) {
                    fireflyObjectIdsMap.replace(key, LazyIdTransform.create(value, graph, type));
                }
            }
        });
    }

    public long getTypeHint(final Object id) {
        return VERTEX_ID_TYPE_TO_HINT.get(id.getClass());
    }

    public long getTypeHint(final Class<?> objectClass) {
        return VERTEX_ID_TYPE_TO_HINT.get(objectClass);
    }

    // For testing purposes only
    public FireflyId getTestId(final Object id) {
        return FireflyIdPoly.fromObject(id, db.getConfig().testSet);
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
