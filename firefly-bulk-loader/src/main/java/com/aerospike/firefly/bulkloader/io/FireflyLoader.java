package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.structure.BulkLoaderEdge;
import com.aerospike.firefly.bulkloader.structure.BulkLoaderVertex;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;

import java.util.ArrayList;
import java.util.List;

public class FireflyLoader implements AutoCloseable {
    private final FireflyGraph graph;

    public FireflyLoader(final FireflyGraph graph) {
        this.graph = graph;
    }

    public void close() {
        this.graph.close();
    }

    public FireflyVertex loadVertex(final BulkLoaderVertex vertex) {
        return this.graph.writeVertex(vertex.getId(), vertex.getLabel(), vertex.getProperties());
    }

    public List<FireflyVertex> loadVertexes(final List<BulkLoaderVertex> vertexes) {
        final List<FireflyVertex> loadedVertexes = new ArrayList<>();
        for (final BulkLoaderVertex vertex : vertexes) {
            final FireflyVertex loadedVertex = loadVertex(vertex);
            loadedVertexes.add(loadedVertex);
        }
        return loadedVertexes;
    }

    public FireflyEdge loadEdge(final BulkLoaderEdge edge) {
        return this.graph.writeEdge(edge.getId(), edge.getLabel(), edge.getProperties(), edge.getFrom(), edge.getTo());
    }

    public List<FireflyEdge> loadEdges(final List<BulkLoaderEdge> edges) {
        final List<FireflyEdge> loadedEdges = new ArrayList<>();
        for (final BulkLoaderEdge edge : edges) {
            final FireflyEdge loadedEdge = loadEdge(edge);
            loadedEdges.add(loadedEdge);
        }
        return loadedEdges;
    }
}
