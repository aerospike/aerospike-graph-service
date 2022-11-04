package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdFactory {
    // Map from classes we support as TinkerPop ids to on disk type hints. NOTE: String not fully supported yet.
    private static final Map<Class<? extends Serializable>, Long> TYPE_TO_IDX = new HashMap<>() {{
        put(Long.class, 1L);
        put(Integer.class, 2L);
        put(String.class, 5L);
    }};
    private static final Map<Long, Class<? extends Serializable>> IDX_TO_TYPE = new HashMap<>() {{
        put(null, null);
        put(0L, null);
        put(1L, Long.class);
        put(2L, Integer.class);
        put(5L, String.class);
    }};

    /**
     * Create an id for a specific FireflyElement type.
     *
     * @param type  Type to create element for.
     * @param id    Id to use for element.
     * @return FireflyId.
     */
    public static FireflyId createFromUser(final Class<? extends FireflyElement> type, final Object id) {
        // Should ask id factories
        boolean supportedType = TYPE_TO_IDX.containsKey(id.getClass());
        Number numericId;
        if (id instanceof String) {
            try {
                numericId = Long.parseLong((String) id);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("String id must be a number.");
            }
        } else {
            numericId = (Number) id;
        }

        if (!FireflyVertex.class.isAssignableFrom(type) &&
                !FireflyEdge.class.isAssignableFrom(type) &&
                !FireflyVertexProperty.class.isAssignableFrom(type)) {
            throw new UnsupportedOperationException(type + " not a Firefly Element ");
        } else if (FireflyVertex.class.isAssignableFrom(type) && !supportedType) {
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
     * @param id Id Object.
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
        if (id instanceof FireflyVertex) {
            idObj = ((FireflyVertex) id).id();
        } else if (id instanceof FireflyEdge) {
            idObj = ((FireflyEdge) id).id();
        } else if (id instanceof FireflyVertexProperty) {
            idObj = ((FireflyVertexProperty) id).id();
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

    public static FireflyId createFromKeyValues(final Class<? extends FireflyElement> type, final Object... keyValues) {
        final Optional<Object> id = ElementHelper.getIdValue(keyValues);
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Id not found in keyValues");
        } else {
            return createFromUser(type, id.get());
        }
    }

    public static FireflyId createFromRecord(final AerospikeConnection db, final FireflyRecord record) {
        return createId(record.key().userKey.getObject(), record.record.getLong(db.ID_TYPE));
    }
}