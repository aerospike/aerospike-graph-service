package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeIdStepLocal extends VertexStep<Vertex> implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FireflyCompositeIdStepLocal.class);
    private final Direction direction;
    private final Set<String> edgeLabels;

    // HasContainers to apply to the read of the composite id step to filter results.
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;
    private final List<String> requiredProperties;
    final Set<String> labels;

    private transient final Map<FireflyId, FireflyVertex> cache = new HashMap<>();
    private transient final List<Traverser.Admin<Vertex>> inputCache = new ArrayList<>();
    private boolean first = true;

    public FireflyCompositeIdStepLocal(final Traversal.Admin traversal,
                                       final Direction direction,
                                       final String[] edgeLabels,
                                       final Set<String> labels,
                                       final List<HasContainer> hasContainers,
                                       final List<String> requiredProperties) {
        super(traversal, Vertex.class, direction, edgeLabels);
        this.direction = direction;
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.labels = new HashSet<>(labels);
        if (hasContainers != null) {
            final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities =
                    FireflyBatchReadHelper.getHasContainersWithCardinalityOrder((FireflyGraph) traversal.getGraph().get(), Vertex.class, hasContainers);
            // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
            //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
            //  matter what.
            fireflyHasContainers = hasContainerWithCardinalities.stream().map(a -> a.hasContainer).collect(Collectors.toList());
            aerospikeHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(hasContainerWithCardinalities);
        } else {
            fireflyHasContainers = List.of();
            aerospikeHasContainers = List.of();
        }
        this.requiredProperties = requiredProperties;
        for (final String label : labels) {
            this.addLabel(label);
        }
    }

    @Override
    public void addStart(final Traverser.Admin<Vertex> start) {
        super.addStart(start);
        if (inputCache.isEmpty()) {
            final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
            graph.logMessage("adding first start.", LOGGER);
        }
        inputCache.add(start);
        first = true;
    }

    private void precompute() {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<?>> fireflyCompositeIdStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new TreeMap<>();

        graph.logMessage("Precompute starting " + inputCache.size(), LOGGER);
        for (final Traverser.Admin<Vertex> traverser : inputCache) {
            // Get next input traverser and get the FireflyVertex form of it.
            final FireflyVertex vertex = (FireflyVertex) traverser.get();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // All the work for supernode scan/index/cache handling is done in the getVertexIdsFromVertex function.
            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);

            FireflyBatchReadHelper.addElementsToSet(
                    fireflyIdList, uniqueIdSet, fireflyVertexMap, vertex.getVertexIdsFromVertex(direction, edgeLabels));

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyCompositeIdStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(traverser, fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output.
                FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                        fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, cache, graph::readVertices, requiredProperties);
            }
        }

        // Drain data to output.
        FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, cache, graph::readVertices, requiredProperties);
        graph.logMessage("Precompute complete " + cache.size(), LOGGER);
        inputCache.clear();
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<Vertex> traverser) {
        // final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        if (!traversal.isRoot() && !(traversal.getParent() instanceof TraversalVertexProgramStep)) {
            inputCache.add(traverser);
            precompute();
        } else if (first) {
            cache.clear();
            precompute();
            first = false;
        }

        final List<Vertex> output = new ArrayList<>();
        final FireflyVertex fireflyVertex = traverser.get() instanceof FireflyVertex
                ? (FireflyVertex) traverser.get()
                : (FireflyVertex) ((ComputerGraph.ComputerVertex) traverser.get()).getBaseVertex();

        // all valid vertices should be in cache
        fireflyVertex.getVertexIdsFromVertex(direction, edgeLabels).forEachRemaining(id -> {
            if (cache.containsKey(id)) {
                output.add(cache.get(id));
            }
        });

        return FireflyCloseableIteratorUtils.filter(output.iterator(), v -> HasContainer.testAll(v, fireflyHasContainers));
    }
}
