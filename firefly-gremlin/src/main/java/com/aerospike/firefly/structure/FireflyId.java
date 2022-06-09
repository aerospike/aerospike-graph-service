package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.util.Optional;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyId {
    private final Class<? extends Serializable> userClass;
    private final Class<? extends Serializable> storageClass;
    private final Object value;
    private final Class<? extends FireflyElement> type;
    private final AerospikeConnection db;

    public static FireflyId fromElement(FireflyElement ele) {
        return ele.id;
    }

    public static FireflyId of(AerospikeConnection db, Class<? extends FireflyElement> type, Object id) {
        return new FireflyId(db, type,id);
    }

    public static FireflyId fromKeyValues(Object[] keyValues) {
        return null;
    }


    public Object value() {
        return value;
    }

    private FireflyId(AerospikeConnection db, Class<? extends FireflyElement> type, Object value) {
        this.db = db;
        this.type = type;
        this.userClass = null;
        this.storageClass = null;
        this.value = value;
    }

    public static FireflyId createFromUser(FireflyGraph graph, Class<? extends FireflyElement> type, Object id) {
        if (type == null)//@todo better verification for free-form
            return new FireflyId(graph.db,type, id);
        if (FireflyVertex.class.isAssignableFrom(type)) {
            if (!graph.vertexIdManager.allow(id))
                throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return new FireflyId(graph.db,type, id);
        } else if (FireflyEdge.class.isAssignableFrom(type)) {
            if (!graph.edgeIdManager.allow(id))
                throw Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return new FireflyId(graph.db,type, id);
        } else if (FireflyVertexProperty.class.isAssignableFrom(type)) {
            if (!graph.vertexPropertyIdManager.allow(id))
                throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return new FireflyId(graph.db,type, id);
        } else throw new UnsupportedOperationException(type + " not a Firefly Element ");
    }

    public static FireflyId createFromManager(FireflyGraph graph, Class<? extends FireflyElement> type) {
        if (FireflyVertex.class.isAssignableFrom(type))
            return new FireflyId(graph.db, type, graph.vertexIdManager.getNextId(graph));
        if (FireflyEdge.class.isAssignableFrom(type))
            return new FireflyId(graph.db, type, graph.edgeIdManager.getNextId(graph));
        if (FireflyVertexProperty.class.isAssignableFrom(type))
            return new FireflyId(graph.db, type, graph.vertexPropertyIdManager.getNextId(graph));
        else throw new UnsupportedOperationException(type + " not a Firefly Element ");
    }

    public static FireflyId loadFromAerospike(AerospikeConnection db, Class<? extends FireflyElement> type, FireflyRecord record) {
        long dbId = record.key().userKey.toLong();
        long dbTypeIdx = record.record.getLong(db.ID_TYPE);
        Object id = FireflyRecord.idStorageTypeToOriginalType(dbId, dbTypeIdx);
        return new FireflyId(db, type, id);
    }

    public static FireflyId createFromKeyValuesOrManager(FireflyGraph graph, Class<? extends FireflyElement> type, Object... keyValues) {
        Optional<Object> maybeId = ElementHelper.getIdValue(keyValues);
        if (maybeId.isPresent())
            return createFromUser(graph, type, maybeId.get());
        else
            return createFromManager(graph, type);
    }

}
