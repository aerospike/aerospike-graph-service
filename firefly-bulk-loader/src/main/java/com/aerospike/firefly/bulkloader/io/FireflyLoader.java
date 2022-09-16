package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.structure.BulkLoaderEdge;
import com.aerospike.firefly.bulkloader.structure.BulkLoaderVertex;
import com.aerospike.firefly.bulkloader.util.EdgeIdHandler;
import com.aerospike.firefly.bulkloader.util.IdHandler;
import com.aerospike.firefly.bulkloader.util.VertexIdHandler;
import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FireflyLoader implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyLoader.class);
    private final FireflyGraph graph;
    private final IdHandler edgeIdHandler;
    private final IdHandler vertexIdHandler;
    private final EdgeReader edgeReader;
    private final VertexReader vertexReader;
    private final Map<Long, Map<String, List<Long>>> vertexIdToOutEdgesLabelIdsMap = new HashMap<>();
    private final Map<Long, Map<String, List<Long>>> vertexIdToInEdgesLabelIdsMap = new HashMap<>();
    private final Set<Long> cacheDisabledVertexes = new HashSet<>();
    private boolean isClosed = false;

    private final List<Long> fireflyEdgeLoadTimes = new ArrayList<>();
    private Long totalEdgeLoadTime = 0L;
    private final List<Long> fireflyVertexLoadTimes = new ArrayList<>();
    private long totalVertexLoadTime = 0L;


    public FireflyLoader(final FireflyGraph graph, final File edgeDirectory, final File vertexDirectory,
                         final long bufferSize) {
        this.graph = graph;
        this.edgeIdHandler = new EdgeIdHandler(graph, bufferSize);
        this.vertexIdHandler = new VertexIdHandler(graph, bufferSize);
        this.edgeReader = new EdgeReader(edgeDirectory);
        this.vertexReader = new VertexReader(vertexDirectory);
    }

    public void close() {
        if (this.isClosed) {
            return;
        }
        this.isClosed = true;
        this.edgeReader.close();
        this.vertexReader.close();
    }

    public void load() {
        if (isClosed) {
            throw new IllegalStateException("Cannot invoke load more than once.");
        }
        try {
            final long start = System.nanoTime();
            loadEdges();
            final long edgesDone = System.nanoTime();
            loadVertexes();
            final long vertexesDone = System.nanoTime();
            this.totalEdgeLoadTime = edgesDone - start;
            this.totalVertexLoadTime = vertexesDone - edgesDone;
        } finally {
            close();
        }
    }

    public void logMetrics() {
        Long totalFireflyEdgeLoadTime = 0L;
        for (final Long l : this.fireflyEdgeLoadTimes) {
            totalFireflyEdgeLoadTime += l;
        }
        final long averageFireflyEdgeLoadTime = totalFireflyEdgeLoadTime / this.fireflyEdgeLoadTimes.size();

        Long totalFireflyVertexLoadTime = 0L;
        for (final Long l : this.fireflyVertexLoadTimes) {
            totalFireflyVertexLoadTime += l;
        }
        final long averageFireflyVertexLoadTime = totalFireflyVertexLoadTime / this.fireflyVertexLoadTimes.size();

        LOG.info("===Loaded a total of " + this.fireflyEdgeLoadTimes.size() + " edges===");
        LOG.info("Average Firefly time in ms: " + averageFireflyEdgeLoadTime / 1000000.0);
        LOG.info("Total Firefly time in ms: " + totalFireflyEdgeLoadTime / 1000000.0);
        LOG.info("Bulk loader overhead time in ms: " + (this.totalEdgeLoadTime - totalFireflyEdgeLoadTime) / 1000000.0);

        LOG.info("===Loaded a total of " + this.fireflyVertexLoadTimes.size() + " vertexes===");
        LOG.info("Average Firefly time in ms: " + averageFireflyVertexLoadTime / 1000000.0);
        LOG.info("Total Firefly time in ms: " + totalFireflyVertexLoadTime / 1000000.0);
        LOG.info("Total bulk loader overhead time in ms : " + (this.totalVertexLoadTime - totalFireflyVertexLoadTime) / 1000000.0);
    }

    private void loadVertex(final BulkLoaderVertex vertex) {
        final long vertexId = this.vertexIdHandler.getId(vertex.getId());
        final Map<String, List<Long>> outEdges =
                this.vertexIdToOutEdgesLabelIdsMap.getOrDefault(vertexId, Collections.emptyMap());
        final Map<String, List<Long>> inEdges =
                this.vertexIdToInEdgesLabelIdsMap.getOrDefault(vertexId, Collections.emptyMap());

        final long start = System.nanoTime();
        this.graph.bulkWriteVertex(vertexId, vertex.getLabel(), vertex.getProperties(), outEdges, inEdges,
                this.cacheDisabledVertexes.contains(vertexId));
        this.fireflyVertexLoadTimes.add(System.nanoTime() - start);
    }

    private void loadVertexes() {
        BulkLoaderVertex vertex;
        while ((vertex = this.vertexReader.next()) != null) {
            loadVertex(vertex);
        }
    }

    private void loadEdge(final BulkLoaderEdge edge) {
        final long edgeId = this.edgeIdHandler.getId(edge.getId());
        final long fromVertexId = this.vertexIdHandler.getId(edge.getFromId());
        final long toVertexId = this.vertexIdHandler.getId(edge.getToId());

        // Record this edge to the list of edges which have the vertex as an IN
        recordVertexEdges(edgeId, edge.getLabel(), fromVertexId, this.vertexIdToInEdgesLabelIdsMap);

        // Record this edge to the list of edges which have the vertex as an OUT
        recordVertexEdges(edgeId, edge.getLabel(), toVertexId, this.vertexIdToOutEdgesLabelIdsMap);

        final long start = System.nanoTime();
        this.graph.bulkWriteEdge(edgeId, edge.getLabel(), edge.getProperties(), fromVertexId, toVertexId);
        this.fireflyEdgeLoadTimes.add(System.nanoTime() - start);
    }

    private void loadEdges() {
        BulkLoaderEdge edge;
        while ((edge = this.edgeReader.next()) != null) {
            loadEdge(edge);
        }
    }

    private void recordVertexEdges(final long edgeId, final String edgeLabel, final long vertexId,
                                   final Map<Long, Map<String, List<Long>>> vertexEdgeRecords) {
        if (!this.cacheDisabledVertexes.contains(vertexId)) {
            final Map<String, List<Long>> edgeLabelsToEdgeIds = vertexEdgeRecords.getOrDefault(vertexId, new HashMap<>());
            final List<Long> edgeIds = edgeLabelsToEdgeIds.getOrDefault(edgeLabel, new ArrayList<>());
            edgeIds.add(edgeId);
            edgeLabelsToEdgeIds.put(edgeLabel, edgeIds);
            vertexEdgeRecords.put(vertexId, edgeLabelsToEdgeIds);
            long totalSize = 0;
            for (final Map.Entry<Long, Map<String, List<Long>>> vertexEdgeRecord : vertexEdgeRecords.entrySet()) {
                for (final Map.Entry<String, List<Long>> edgeLabelToEdgeIds : vertexEdgeRecord.getValue().entrySet()) {
                    totalSize += edgeLabelToEdgeIds.getValue().size();
                }
            }
            if (totalSize > graph.getBaseGraph().ID_CACHE_SIZE) {
                this.cacheDisabledVertexes.add(vertexId);
                vertexEdgeRecords.put(vertexId, Collections.emptyMap());
            }
        }
    }
}
