package com.aerospike.firefly.structure;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
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
public class FireflyID {
    private final Class<? extends Serializable> userClass;
    private final Class<? extends Serializable> storageClass;
    private final Object value;
    private final FireflyGraph graph;
    private final Class<? extends FireflyElement> type;

    private FireflyID(FireflyGraph graph, Class<? extends FireflyElement> type, Object value) {
        this.graph = graph;
        this.type = type;
        this.userClass = null;
        this.storageClass = null;
        this.value = value;
    }

    public FireflyID fromUser(FireflyGraph graph, Class<? extends FireflyElement> type, Object id) {
        if (FireflyVertex.class.isAssignableFrom(type)) {
            if (!graph.vertexIdManager.allow(id))
                throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return new FireflyID(graph, type, id);
        } else if (FireflyEdge.class.isAssignableFrom(type)) {
            if (!graph.edgeIdManager.allow(id))
                throw Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return new FireflyID(graph, type, id);
        } else if (FireflyVertexProperty.class.isAssignableFrom(type)) {
            if (!graph.vertexPropertyIdManager.allow(id))
                throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            return new FireflyID(graph, type, id);
        }
        else throw new UnsupportedOperationException(type + " not a Firefly Element ");
    }

    public FireflyID fromManager(FireflyGraph graph, Class<? extends FireflyElement> type) {
        if (FireflyVertex.class.isAssignableFrom(type))
            return new FireflyID(graph, type, graph.vertexIdManager.getNextId(graph));
        if (FireflyEdge.class.isAssignableFrom(type))
            return new FireflyID(graph, type, graph.edgeIdManager.getNextId(graph));
        if (FireflyVertexProperty.class.isAssignableFrom(type))
            return new FireflyID(graph, type, graph.vertexPropertyIdManager.getNextId(graph));
        else throw new UnsupportedOperationException(type + " not a Firefly Element ");

    }

    public FireflyID fromAerospike(FireflyGraph graph, Class<? extends FireflyElement> type, Key key, Record record) {
        long dbId = key.userKey.toLong();
        long dbTypeIdx = record.getLong(graph.db.ID_TYPE);
        Object id = FireflyRecord.idStorageTypeToOriginalType(dbId, dbTypeIdx);
        return new FireflyID(graph, type, id);
    }

    public FireflyID fromKeyValuesOrManager(FireflyGraph graph, Class<? extends FireflyElement> type, Object... keyValues) {
        Optional<Object> maybeId = ElementHelper.getIdValue(keyValues);
        if (maybeId.isPresent())
            return fromUser(graph, type, maybeId.get());
        else
            return fromManager(graph, type);
    }

    public Key getKey() {
        return null;
    }


}
