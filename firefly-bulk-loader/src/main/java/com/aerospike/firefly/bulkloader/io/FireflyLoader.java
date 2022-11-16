package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.structure.BulkLoaderEdge;
import com.aerospike.firefly.bulkloader.structure.BulkLoaderVertex;
import com.aerospike.firefly.bulkloader.util.EdgeIdHandler;
import com.aerospike.firefly.bulkloader.util.IdHandler;
import com.aerospike.firefly.bulkloader.util.VertexIdHandler;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import org.apache.commons.configuration2.Configuration;
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

import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.ID_BUFFER_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.ID_PROPERTY_NAME_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.USE_PROVIDED_ID_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.getOrDefault;

public class FireflyLoader implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyLoader.class);
    private final FireflyGraph graph;
    private final IdHandler edgeIdHandler;
    private final IdHandler vertexIdHandler;
    private final EdgeReader edgeReader;
    private final VertexReader vertexReader;
    private final Map<Long, Map<String, List<FireflyId>>> vertexIdToOutEdgesLabelIdsMap = new HashMap<>();
    private final Map<Long, Map<String, List<FireflyId>>> vertexIdToInEdgesLabelIdsMap = new HashMap<>();
    private final Set<Long> cacheDisabledVertices = new HashSet<>();
    private boolean isClosed = false;

    private final List<Long> fireflyEdgeLoadTimes = new ArrayList<>();
    private Long totalEdgeLoadTime = 0L;
    private final List<Long> fireflyVertexLoadTimes = new ArrayList<>();
    private long totalVertexLoadTime = 0L;


    public FireflyLoader(final FireflyGraph graph, final Configuration config) {
        final boolean useProvidedId = Boolean.parseBoolean(getOrDefault(USE_PROVIDED_ID_KEY, config));
        final String providedIdPropertyName = getOrDefault(ID_PROPERTY_NAME_KEY, config);
        final long bufferSize = Long.parseLong(getOrDefault(ID_BUFFER_KEY, config));
        final File edgeDirectory = new File(getOrDefault(EDGE_DIRECTORY_KEY, config));
        final File vertexDirectory = new File(getOrDefault(VERTEX_DIRECTORY_KEY, config));

        this.graph = graph;
        this.edgeIdHandler = new EdgeIdHandler(graph, bufferSize, useProvidedId);
        this.vertexIdHandler = new VertexIdHandler(graph, bufferSize, useProvidedId);
        this.edgeReader = new EdgeReader(edgeDirectory, providedIdPropertyName, useProvidedId);
        this.vertexReader = new VertexReader(vertexDirectory, providedIdPropertyName, useProvidedId);
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
            loadVertices();
            final long verticesDone = System.nanoTime();
            this.totalEdgeLoadTime = edgesDone - start;
            this.totalVertexLoadTime = verticesDone - edgesDone;
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
        LOG.info("Throughput of edges per second: " + 1000000000 / averageFireflyEdgeLoadTime);

        LOG.info("===Loaded a total of " + this.fireflyVertexLoadTimes.size() + " vertices===");
        LOG.info("Average Firefly time in ms: " + averageFireflyVertexLoadTime / 1000000.0);
        LOG.info("Total Firefly time in ms: " + totalFireflyVertexLoadTime / 1000000.0);
        LOG.info("Total bulk loader overhead time in ms : " + (this.totalVertexLoadTime - totalFireflyVertexLoadTime) / 1000000.0);
        LOG.info("Throughput of vertices per second: " + 1000000000 / averageFireflyVertexLoadTime);
    }

    private void loadVertex(final BulkLoaderVertex vertex) {
        final long vertexId = this.vertexIdHandler.getId(vertex.getId());
        final Map<String, List<FireflyId>> outEdges =
                this.vertexIdToOutEdgesLabelIdsMap.getOrDefault(vertexId, Collections.emptyMap());
        final Map<String, List<FireflyId>> inEdges =
                this.vertexIdToInEdgesLabelIdsMap.getOrDefault(vertexId, Collections.emptyMap());

        final long start = System.nanoTime();
        this.graph.bulkWriteVertex(vertexId, vertex.getLabel(), vertex.getProperties(), outEdges, inEdges,
                this.cacheDisabledVertices.contains(vertexId));
        this.fireflyVertexLoadTimes.add(System.nanoTime() - start);
    }

    private void loadVertices() {
        BulkLoaderVertex vertex;
        while ((vertex = this.vertexReader.next()) != null) {
            loadVertex(vertex);
        }
    }

    private void loadEdge(final BulkLoaderEdge edge) {
        final long edgeId = this.edgeIdHandler.getId(edge.getId());
        final long fromVertexId = this.vertexIdHandler.getId(edge.getFromId());
        final long toVertexId = this.vertexIdHandler.getId(edge.getToId());

        // Record this edge to the set of outgoing edges of the "from" vertex
        recordVertexEdges(edgeId, edge.getLabel(), fromVertexId, toVertexId, fromVertexId, this.vertexIdToOutEdgesLabelIdsMap);

        // Record this edge to the set of incoming edges of the "to" vertex
        recordVertexEdges(edgeId, edge.getLabel(), fromVertexId, toVertexId, toVertexId, this.vertexIdToInEdgesLabelIdsMap);

        final long start = System.nanoTime();
        this.graph.bulkWriteEdge(edgeId, edge.getLabel(), edge.getProperties(), toVertexId, fromVertexId);
        this.fireflyEdgeLoadTimes.add(System.nanoTime() - start);
    }

    private void loadEdges() {
        BulkLoaderEdge edge;
        while ((edge = this.edgeReader.next()) != null) {
            loadEdge(edge);
        }
    }

    private void recordVertexEdges(final long edgeId, final String edgeLabel, final long inVertex, final long outVertex, final long vertexId,
                                   final Map<Long, Map<String, List<FireflyId>>> vertexEdgeRecords) {
        if (!this.cacheDisabledVertices.contains(vertexId)) {
            final FireflyId ffIdInVertex = FireflyIdFactory.createId(inVertex);
            final FireflyId ffIdOutVertex = FireflyIdFactory.createId(outVertex);
            final FireflyId ffIdEdge = FireflyIdFactory.createId(edgeId);
            final FireflyId compositeId = FireflyIdFactory.createEdgeId(ffIdEdge, ffIdInVertex, ffIdOutVertex);

            final Map<String, List<FireflyId>> edgeLabelsToEdgeIds = vertexEdgeRecords.getOrDefault(vertexId, new HashMap<>());
            final List<FireflyId> edgeIds = edgeLabelsToEdgeIds.getOrDefault(edgeLabel, new ArrayList<>());
            edgeIds.add(compositeId);
            edgeLabelsToEdgeIds.put(edgeLabel, edgeIds);
            vertexEdgeRecords.put(vertexId, edgeLabelsToEdgeIds);
            long totalSize = 0;
            for (final Map.Entry<Long, Map<String, List<FireflyId>>> vertexEdgeRecord : vertexEdgeRecords.entrySet()) {
                for (final Map.Entry<String, List<FireflyId>> edgeLabelToEdgeIds : vertexEdgeRecord.getValue().entrySet()) {
                    totalSize += edgeLabelToEdgeIds.getValue().size();
                }
            }
            if (totalSize > graph.getBaseGraph().ID_CACHE_SIZE) {
                this.cacheDisabledVertices.add(vertexId);
                vertexEdgeRecords.put(vertexId, Collections.emptyMap());
            }
        }
    }
}
