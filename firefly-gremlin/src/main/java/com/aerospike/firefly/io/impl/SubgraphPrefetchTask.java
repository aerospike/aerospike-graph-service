package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.EgoNetwork;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.PrefetchTask;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyReadThroughCacheStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class SubgraphPrefetchTask implements PrefetchTask {
    private static final Logger LOG = LoggerFactory.getLogger(SubgraphPrefetchTask.class);

    private final FireflyCache cache;

    private SubgraphPrefetchTask(final FireflyCache cache) {
        this.cache = cache;
    }

    public static PrefetchTask create(final FireflyCache cache) {
        return new SubgraphPrefetchTask(cache);
    }

    /**
     * If this prefetch task supports prefetching the supplied Traversal, return
     * a Runnable that when executed, will fill the cache with the records it expects
     * to be required by the traversal
     * If it does not match the traversal, return an empty Optional
     *
     * @param traversal the current Traversal
     * @return Optional of Runnable
     */
    @Override
    public Optional<Runnable> getTask(final Traversal.Admin<?, ?> traversal) {
        if (!GraphStep.class.isAssignableFrom(traversal.getStartStep().getNextStep().getClass())) {
            return Optional.empty();
        }

        // Do we have a specific starting point? If not, don't run.
        final GraphStep startStep = (GraphStep) traversal.getStartStep().getNextStep();
        if (startStep.getIds() == null || startStep.getIds().length != 1) {
            return Optional.empty();
        }

        // Get the graph, if it does not exist exit.
        final Optional<Graph> optionalGraph = traversal.getGraph();
        if (optionalGraph.isEmpty()) {
            return Optional.empty();
        }
        final FireflyGraph graph = (FireflyGraph) optionalGraph.get();

        // If cache doesn't exist remove it.
        if (cache == null) {
            return Optional.empty();
        }

        // Out steps are the only ones we can prefetch at this time.
        final List<Step> outSteps = traversal
                .getSteps()
                .stream()
                .filter(step ->
                        VertexStep.class.isAssignableFrom(step.getClass()))
                .filter(vertexStep ->
                        ((VertexStep<?>) vertexStep).getDirection() == Direction.OUT)
                .collect(Collectors.toList());

        // TODO: This is too simple, it should be 2 out steps off the starting point.
        if (outSteps.size() < 2) {
            return Optional.empty();
        }

        // Get ReadThroughCache.
        final Object startVertexId = ((GraphStep) traversal.getStartStep().getNextStep()).getIds()[0];
        // Fetch the ego network of the egoId and then fetch the EgoNetwork of each adjacent Vertex.
        final Runnable primeCacheOperation = () -> EgoNetwork.create(FireflyIdFactory.createId(startVertexId), graph)
                .vertexRecords
                .forEach(kr -> {
                    cache.insert(kr.key, kr.record);
                    EgoNetwork.create(FireflyIdFactory.createId(kr.key.userKey.toLong()), graph)
                            .records()
                            .forEachRemaining(subKr -> cache.insert(subKr.key, subKr.record));
                });
        return Optional.of(primeCacheOperation);
    }

}
