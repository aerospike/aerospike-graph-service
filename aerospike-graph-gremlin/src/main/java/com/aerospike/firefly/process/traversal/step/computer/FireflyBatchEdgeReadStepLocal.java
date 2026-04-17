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

package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FireflyBatchEdgeReadStepLocal extends VertexStep<Edge> {
    private final Direction direction;
    private final Set<String> edgeLabels;

    // HasContainers to apply to the read of the composite id step to filter results.
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;
    final Set<String> labels;

    private transient final Map<FireflyId, FireflyEdge> cache = new HashMap<>();
    private transient final List<Traverser.Admin<Vertex>> inputCache = new ArrayList<>();
    private boolean first = true;
    private final boolean areEdgesRequired;

    public FireflyBatchEdgeReadStepLocal(final Traversal.Admin traversal,
                                         final Direction direction,
                                         final String[] edgeLabels,
                                         final Set<String> labels,
                                         final List<HasContainer> hasContainers,
                                         final boolean areEdgesRequired) {
        super(traversal, Edge.class, direction, edgeLabels);
        this.direction = direction;
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.labels = new HashSet<>(labels);
        this.areEdgesRequired = areEdgesRequired;
        // GRAPH-401: keep the full list as the post-read filter because the fresh readEdges call below
        // passes Collections.emptyList() as its aerospike containers (no server-side push-down),
        // so every predicate must still be applied client-side on the returned edges.
        final FireflyBatchReadHelper.SplitHasContainers split = FireflyBatchReadHelper.splitHasContainers(
                (FireflyGraph) traversal.getGraph().get(), Edge.class, hasContainers);
        this.fireflyHasContainers = split.all;
        this.aerospikeHasContainers = split.aerospike;
        for (final String label : labels) {
            this.addLabel(label);
        }
    }

    @Override
    public void addStart(final Traverser.Admin<Vertex> start) {
        super.addStart(start);
        inputCache.add(start);
        first = true;
    }

    private void precompute() {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<?>> fireflyBatchEdgeReadStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyEdge> fireflyEdgeMap = new HashMap<>();

        for (final Traverser.Admin<Vertex> traverser : inputCache) {
            // Get next input traverser and get the FireflyVertex form of it.
            final FireflyVertex vertex = (FireflyVertex) traverser.get();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // All the work for supernode scan/index/cache handling is done in the getVertexIdsFromVertex function.
            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);

            vertex.getBatchedEdgeIdsFromVertex(direction, edgeLabels, fireflyIdList, aerospikeHasContainers, (List<HasContainer>)null);
            for (int i = previousSize; i < fireflyIdList.size(); i++) {
                final FireflyId id = fireflyIdList.get(i);
                if (!fireflyEdgeMap.containsKey(id)) {
                    uniqueIdSet.add(id);
                }
            }

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyBatchEdgeReadStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(traverser, fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().getConfig().aerospikeBatchReadSize ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().getConfig().aerospikeBatchReadSize) {
                // Drain data to output. No need to pass in aerospikeHasContainers since they were used to filter Edge IDs already.
                FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                        fireflyEdgeMap, fireflyBatchEdgeReadStepInfos, Collections.emptyList(), fireflyHasContainers, cache, graph::readEdges, null, areEdgesRequired);
            }
        }

        FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                fireflyEdgeMap, fireflyBatchEdgeReadStepInfos, Collections.emptyList(), fireflyHasContainers, cache, graph::readEdges, null, areEdgesRequired);
    }

    @Override
    protected Iterator<Edge> flatMap(final Traverser.Admin<Vertex> traverser) {
        if (!traversal.isRoot() && !(traversal.getParent() instanceof TraversalVertexProgramStep)) {
            inputCache.add(traverser);
            precompute();
        } else if (first) {
            cache.clear();
            precompute();
            first = false;
        }

        final List<Edge> output = new ArrayList<>();
        FireflyVertex fireflyVertex = (FireflyVertex) traverser.get();
        // all valid vertices should be in cache
        fireflyVertex.getEdgeIdsFromVertex(direction, edgeLabels, aerospikeHasContainers).forEachRemaining(id -> {
            if (cache.containsKey(id)) {
                output.add(cache.get(id));
            }
        });

        return output.iterator();
    }
}
