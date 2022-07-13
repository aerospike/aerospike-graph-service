package com.aerospike.firefly.io;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface Backend {
    public static interface VertexProperty{

    }
    public static interface Vertex{

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
    public static interface Edge{

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
    }
}
