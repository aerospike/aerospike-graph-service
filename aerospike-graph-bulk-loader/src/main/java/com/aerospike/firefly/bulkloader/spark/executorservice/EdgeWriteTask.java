/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.graph.GraphOperations;
import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EdgeWriteTask {
    final ExponentialBackoffRetry retry;
    private final Logger LOGGER = LoggerFactory.getLogger(EdgeWriteTask.class);
    private final Set<Object> supernodes;
    private final boolean keepProvidedId;
    private final String providedIdPropertyName;
    private final String nullValue;
    private final FireflyGraph graph;
    private final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap;
    private final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap;
    private final GenericRowWithSchema fireflyRow;
    private final GenericRowWithSchema fireflyMetadataRow;
    private final boolean edgeCacheEnabled;
    public final FireflyId edgeId;
    public final SparkFireflyEdge sparkEdge;
    public final Object inVertexId;
    public final Object outVertexId;
    private final String edgeLabel;
    private final boolean inVertexSupernode;
    private final boolean outVertexSupernode;
    private final int partitionId;

    public EdgeWriteTask(
            final ExponentialBackoffRetry retry,
            final Set<Object> supernodes,
            final boolean keepProvidedId,
            final String providedIdPropertyName,
            final String nullValue,
            final FireflyGraph graph,
            final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap,
            final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap,
            final GenericRowWithSchema rowForFirefly,
            final GenericRowWithSchema fireflyMetadataRow,
            final boolean usePersistedEdgeId,
            final int partitionId) {
        this.retry = retry;
        this.supernodes = supernodes;
        this.keepProvidedId = keepProvidedId;
        this.providedIdPropertyName = providedIdPropertyName;
        this.nullValue = nullValue;
        this.graph = graph;
        this.vertexOutEdgeMap = vertexOutEdgeMap;
        this.vertexInEdgeMap = vertexInEdgeMap;
        this.fireflyRow = rowForFirefly;
        this.fireflyMetadataRow = fireflyMetadataRow;
        this.edgeCacheEnabled = this.graph.getBaseGraph().getConfig().globalEdgeCacheEnabledFlag;
        sparkEdge = SparkFireflyEdge.createEdge(fireflyRow, keepProvidedId,
                providedIdPropertyName, nullValue, graph, false,
                EdgeOperations.getEdgeIdSupplied(fireflyMetadataRow, usePersistedEdgeId));
        edgeId = sparkEdge.getFireflyId(this.graph.getBaseGraph());
        inVertexId = sparkEdge.getInVertexId();
        outVertexId = sparkEdge.getOutVertexId();
        edgeLabel = sparkEdge.getLabel();
        // If the edge cache is not enabled, then every edge must be written as if it were attached to a supernode.
        inVertexSupernode = !edgeCacheEnabled || supernodes.contains(inVertexId);
        outVertexSupernode = !edgeCacheEnabled || supernodes.contains(outVertexId);
        this.partitionId = partitionId;
    }

    public CompletionStage<Void> write(final ScheduledExecutorService service) {
        final Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.supplyAsync(() -> {
            this.graph.bulkWriteEdge((byte[]) sparkEdge.getId(), edgeLabel, sparkEdge.getProperties(),
                    inVertexId, outVertexId, inVertexSupernode, outVertexSupernode, partitionId);
            return null;
        }, service);
        return retry.withRetries(supplier, service).exceptionally(e -> {
            // Log the error when no longer retrying
            LOGGER.error(String.format("Exception occurred during Edge writing %s", this), e);
            throw new RuntimeException(e);
        });
    }

    public static CompletionStage<Void> writeBatch(final ScheduledExecutorService service,
                                                   final FireflyGraph graph,
                                                   final List<EdgeWriteTask> edgeWriteTask) {
        if (edgeWriteTask.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        final List<byte[]> ids = edgeWriteTask.stream().map(task -> (byte[]) task.sparkEdge.getId()).collect(Collectors.toList());
        final List<String> edgeLabels = edgeWriteTask.stream().map(task -> task.edgeLabel).collect(Collectors.toList());
        final List<List<Map.Entry<String, Object>>> properties = edgeWriteTask.stream().map(task -> task.sparkEdge.getProperties()).collect(Collectors.toList());
        final List<Object> inVertexIds = edgeWriteTask.stream().map(task -> task.inVertexId).collect(Collectors.toList());
        final List<Object> outVertexIds = edgeWriteTask.stream().map(task -> task.outVertexId).collect(Collectors.toList());
        final List<Boolean> inVertexSupernodes = edgeWriteTask.stream().map(task -> task.inVertexSupernode).collect(Collectors.toList());
        final List<Boolean> outVertexSupernodes = edgeWriteTask.stream().map(task -> task.outVertexSupernode).collect(Collectors.toList());
        final List<Integer> partitionIds = edgeWriteTask.stream().map(task -> task.partitionId).collect(Collectors.toList());

        final Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.supplyAsync(() -> {
            try {
                graph.bulkWriteEdges(ids, edgeLabels, properties, inVertexIds, outVertexIds, inVertexSupernodes, outVertexSupernodes, partitionIds);
            } catch (final Exception e) {
                // Log the error when no longer retrying
                edgeWriteTask.get(0).LOGGER.error(String.format("Exception occurred during Edge writing %s", edgeWriteTask), e);
                throw new RuntimeException(e);
            }
            return null;
        }, service);
        return edgeWriteTask.get(0).retry.withRetries(supplier, service).exceptionally(e -> {
            // Log the error when no longer retrying
            edgeWriteTask.get(0).LOGGER.error(String.format("Exception occurred during Edge writing %s", edgeWriteTask), e);
            throw new RuntimeException(e);
        });
    }

    public void updateCacheMap() {
        if (edgeCacheEnabled) {
            GraphOperations.updateEdgeMap(supernodes, outVertexId,
                    graph.getIdFactory().createCompositeEdgeId((FireflyEdgeId) edgeId, graph.getIdFactory().createVertexId(inVertexId)),
                    edgeLabel, vertexOutEdgeMap);
            GraphOperations.updateEdgeMap(supernodes, inVertexId,
                    graph.getIdFactory().createCompositeEdgeId((FireflyEdgeId) edgeId, graph.getIdFactory().createVertexId(outVertexId)),
                    edgeLabel, vertexInEdgeMap);
        }
    }

    @Override
    public String toString() {
        return "EdgeWriteTask{" +
                "supernodes=" + supernodes +
                ", keepProvidedId=" + keepProvidedId +
                ", providedIdPropertyName='" + providedIdPropertyName + '\'' +
                ", nullValue='" + nullValue + '\'' +
                ", graph=" + graph +
                ", vertexOutEdgeMap=" + vertexOutEdgeMap +
                ", vertexInEdgeMap=" + vertexInEdgeMap +
                ", metadataRow=" + fireflyMetadataRow +
                '}';
    }
}
