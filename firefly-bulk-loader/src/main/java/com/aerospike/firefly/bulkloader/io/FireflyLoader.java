package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.structure.BulkLoaderEdge;
import com.aerospike.firefly.bulkloader.structure.BulkLoaderVertex;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;

import java.util.ArrayList;
import java.util.List;

public class FireflyLoader implements AutoCloseable {
    private FireflyGraph graph;

    public FireflyLoader(FireflyGraph graph) {
        this.graph = graph;
    }

    public void close() {
        this.graph.close();
    }

    public FireflyVertex loadVertex(BulkLoaderVertex vertex) {
        return this.graph.writeVertex(vertex.getId(), vertex.getLabel(), vertex.getProperties());
    }

    public List<FireflyVertex> loadVertexes(List<BulkLoaderVertex> vertexes) {
        List<FireflyVertex> loadedVertexes = new ArrayList<>();
        for (BulkLoaderVertex vertex : vertexes) {
            FireflyVertex loadedVertex = loadVertex(vertex);
            loadedVertexes.add(loadedVertex);
        }
        return loadedVertexes;
    }

    public FireflyEdge loadEdge(BulkLoaderEdge edge) {
        return this.graph.writeEdge(edge.getId(), edge.getLabel(), edge.getProperties(), edge.getFrom(), edge.getTo());
    }

    public List<FireflyEdge> loadEdges(List<BulkLoaderEdge> edges) {
        List<FireflyEdge> loadedEdges = new ArrayList<>();
        for (BulkLoaderEdge edge : edges) {
            FireflyEdge loadedEdge = loadEdge(edge);
            loadedEdges.add(loadedEdge);
        }
        return loadedEdges;
    }
}
