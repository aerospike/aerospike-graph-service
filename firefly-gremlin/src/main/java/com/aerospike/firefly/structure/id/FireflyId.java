package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.structure.FireflyVertex;
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

    public static FireflyId fromElement(FireflyElement ele) {
        return ele.id;
    }

    public static FireflyId of(Class<? extends FireflyElement> type, Object id) {
        return new FireflyId(type, id);
    }

    public FireflyId toNumericId() {
        return new FireflyId(type, NumericIdManager.convert(value));
    }


    public Object value() {
        return value;
    }

    private FireflyId(Class<? extends FireflyElement> type, Object value) {
        this.type = type;
        this.userClass = null;
        this.storageClass = null;
        this.value = value;
    }

    public static FireflyId createFromUser(FireflyGraph graph, Class<? extends FireflyElement> type, Object id) {
        if (type == null)//@todo better verification for free-form
            return new FireflyId(type, id);
        if (FireflyVertex.class.isAssignableFrom(type)) {
            if (!graph.vertexIdManager.allow(id.getClass()))
                throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return new FireflyId(type, id);
        } else if (FireflyEdge.class.isAssignableFrom(type)) {
            if (!graph.edgeIdManager.allow(id.getClass()))
                throw Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return new FireflyId(type, id);
        } else if (FireflyVertexProperty.class.isAssignableFrom(type)) {
            if (!graph.vertexPropertyIdManager.allow(id.getClass()))
                throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return new FireflyId(type, id);
        } else throw new UnsupportedOperationException(type + " not a Firefly Element ");
    }

    public static FireflyId createFromManager(FireflyGraph graph, Class<? extends FireflyElement> type) {
        if (FireflyVertex.class.isAssignableFrom(type))
            return new FireflyId(type, graph.vertexIdManager.getNextId(graph));
        if (FireflyEdge.class.isAssignableFrom(type))
            return new FireflyId(type, graph.edgeIdManager.getNextId(graph));
        if (FireflyVertexProperty.class.isAssignableFrom(type))
            return new FireflyId(type, graph.vertexPropertyIdManager.getNextId(graph));
        else throw new UnsupportedOperationException(type + " not a Firefly Element ");
    }

    public static FireflyId loadFromAerospike(AerospikeConnection db, Class<? extends FireflyElement> type, FireflyRecord record) {
        long dbId = NumericIdManager.convert(record.key().userKey.getObject());
        long dbTypeIdx = record.record.getLong(db.ID_TYPE);
        Object id = FireflyRecord.idStorageTypeToOriginalType(dbId, dbTypeIdx);
        return new FireflyId(type, id);
    }

    public static FireflyId createFromKeyValuesOrManager(FireflyGraph graph, Class<? extends FireflyElement> type, Object... keyValues) {
        Optional<Object> maybeId = ElementHelper.getIdValue(keyValues);
        return maybeId.map(o -> createFromUser(graph, type, o)).orElseGet(() -> createFromManager(graph, type));
    }

}
