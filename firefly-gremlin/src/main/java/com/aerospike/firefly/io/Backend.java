package com.aerospike.firefly.io;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.structure.*;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Backend {
    interface Index {

        Iterator<FireflyEdge> queryEdgePropertyStringMatchIndex(FireflyGraph graph, String key, Object value);

        Iterator<FireflyEdge> queryEdgePropertyNumericMatchIndex(FireflyGraph graph, String key, P<?> predicate);

        Iterator<FireflyEdge> queryEdgePropertyNumericRangeIndex(FireflyGraph graph, String key, P<?> predicate);

        Iterator<? extends org.apache.tinkerpop.gremlin.structure.Vertex> queryVertexLabelStringIndex(FireflyGraph graph, Object value);

        Iterator<? extends org.apache.tinkerpop.gremlin.structure.Edge> queryEdgeLabelStringIndex(FireflyGraph graph, Object value);

        Iterator<FireflyVertexProperty> queryVertexPropertyStringIndex(FireflyGraph graph, String key, Object value);

        Iterator<FireflyVertexProperty> queryVertexPropertyNumberMatchIndex(FireflyGraph graph, String key, P<?> predicate);

        Iterator<FireflyVertexProperty> queryVertexPropertyNumberRangeIndex(FireflyGraph graph, String key, P<?> predicate);
    }

    interface Graph {

        <V> V readGraphVariable(String key);

        Set<String> readGraphVariableKeys();

        <V> void writeGraphVariable(String key, V value);

        <V> void removeGraphVariable(String key);
    }

    interface Element {

        <V> void writeProperty(FireflyId id, Class<? extends FireflyElement> clazz, String key, V value);

        <V> void removeProperty(FireflyElement element, String key);

        Iterator<?> readElementIds(Class<? extends FireflyElement> type);

        <V> Map<String, Property> readProperties(FireflyElement element);

        <V> Property readProperty(FireflyElement element, String key);
    }

    interface VertexProperty {
        void addVPToVertex(FireflyVertex vertex, FireflyVertexProperty vp);

        <V> FireflyVertexProperty<V> readVertexProperty(FireflyVertex parent, FireflyId vpId);

        <V> FireflyVertexProperty<V> vertexPropertyFromRecord(FireflyGraph graph, FireflyRecord fireflyRecord, FireflyId parentId);

        Map<String, List<org.apache.tinkerpop.gremlin.structure.VertexProperty>> readVertexPropertiesByScan(FireflyVertex vertex);

        List<org.apache.tinkerpop.gremlin.structure.VertexProperty> readVertexProperty(FireflyVertex vertex, String key);

        Map<String, List<org.apache.tinkerpop.gremlin.structure.VertexProperty>> readVertexProperties(FireflyVertex vertex);

        <V> void writeVertexProperty(FireflyVertex vertex,
                                     FireflyId vpid,
                                     String vpk,
                                     String key,
                                     V value);

        void removeIdFromVertexPropertyList(FireflyVertex vertex, org.apache.tinkerpop.gremlin.structure.VertexProperty vp);

        void removeVertexProperty(FireflyVertexProperty property);

        boolean vertexPropertyExists(FireflyId vpId);
    }

    interface Vertex {

        long getVertexCount();

        FireflyVertex readVertex(FireflyGraph graph, FireflyId vertexId);

        void writeVertex(FireflyGraph graph, FireflyId vertexId, String label);

        void removeVertex(FireflyGraph graph, FireflyId vertexId);

        FireflyVertex vertexFromRecord(FireflyGraph graph, FireflyRecord fireflyRecord);

        void writeFullyQualifiedVertex(FireflyGraph graph, FireflyId vertexId, String label, List<Map.Entry<String, Object>> properties);

        Iterator<Object> getInEdgeIdsFromVertex(FireflyVertex vertex);

        Iterator<Object> getOutEdgeIdsFromVertex(FireflyVertex vertex);

        Iterator<Object> getOutEdgeIdsFromVertexByScan(FireflyVertex vertex);

        Map<String, List<Long>> getXXXIdsFromVertexLabelMap(FireflyVertex vertex, String mapName);

        Iterator<Object> getXXXIdsFromVertexByCache(FireflyVertex vertex, String mapName);

        Iterator<Object> getInEdgeIdsFromVertexByScan(FireflyVertex vertex);

        boolean vertexExists(FireflyId vertexId);

        FireflyRecord getVertexRecord(FireflyId id);

        FireflyRecord getVertexRecord(FireflyVertex vertex);

    }

    interface Edge {

        void addEdgeToVertex(FireflyVertex vertex, FireflyId edgeId, String label, Direction direction);

        void removeEdgeFromVertex(FireflyGraph graph, FireflyVertex vertex, FireflyEdge edge, Direction direction);

        void removeEdge(FireflyGraph graph, FireflyId edgeId);

        FireflyEdge readEdge(FireflyGraph graph, FireflyId edgeId);

        FireflyEdge edgeFromRecord(FireflyGraph graph, Key key, Record edgeRecord);

        void writeEdge(FireflyGraph graph,
                       FireflyId edgeId,
                       String label,
                       FireflyVertex outVertex,
                       FireflyVertex inVertex,
                       Object[] keyValues);

        void writeFullyQualifiedEdge(FireflyGraph graph,
                                     FireflyId edgeId,
                                     String label,
                                     FireflyVertex outVertex,
                                     FireflyVertex inVertex,
                                     List<Map.Entry<String, Object>> properties);

        boolean edgeExists(FireflyId edgeId);

        long getEdgeCount();
    }
}
