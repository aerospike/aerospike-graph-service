package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.EgoNetwork;
import com.aerospike.firefly.io.PrefetchTask;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyTraversalCacheStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
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
    public static PrefetchTask create(){
        return new SubgraphPrefetchTask();
    }

    /**
     * If this prefetch task supports prefetching the supplied Traversal, return
     * a Runnable that when executed, will fill the cache with the records it expects
     * to be required by the traversal
     * If it does not match the traversal, return an empty Optional
     * @param traversal the current Traversal
     * @return Optional of Runnable
     */
    @Override
    public Optional<Runnable> getTask(Traversal.Admin<?, ?> traversal) {

        if (!GraphStep.class.isAssignableFrom(traversal.getStartStep().getNextStep().getClass()))
            return Optional.empty();
        Step<?, ?> startStep = traversal.getStartStep().getNextStep();
        //Do we have a specific starting point? if not, don't run
        if (((GraphStep) startStep).getIds() == null)
            return Optional.empty();
        if (((GraphStep) startStep).getIds().length != 1)
            return Optional.empty();



        final FireflyGraph graph;
        final UUID cacheId;
        if (traversal.getGraph().isEmpty())
            return Optional.empty();
        else graph = (FireflyGraph) traversal.getGraph().get();

        if (FireflyTraversalCacheStrategy.Util.idFromTraversal(traversal).isEmpty())
            return Optional.empty();
        else cacheId = FireflyTraversalCacheStrategy.Util.idFromTraversal(traversal).get();

        List<Step> outSteps = traversal
                .getSteps()
                .stream()
                .filter(step ->
                        VertexStep.class.isAssignableFrom(step.getClass()))
                .filter(vertexStep ->
                        ((VertexStep<?>) vertexStep).getDirection() == Direction.OUT)
                .collect(Collectors.toList());
        //@todo this is to simple, it should be 2 out steps off the starting point
        if (outSteps.size() < 2)
            return Optional.empty();
        TraversalCache traversalCache = graph.getBaseGraph().traversalCacheSet.get(cacheId);
        LOG.info("will init cache with id: " + cacheId);
        Object startVertexId = ((GraphStep) traversal.getStartStep().getNextStep()).getIds()[0];
        //Fetch the ego network of the egoId and then fetch the EgoNetwork of each adjacent Vertex
        Runnable primeCacheOperation = () -> {
            EgoNetwork.create(FireflyId.of(FireflyVertex.class, startVertexId), graph)
                    .vertexRecords
                    .stream() // todo: parallelStream
                    .forEach(kr -> {
                        traversalCache.insert(kr.key, kr.record);
                        EgoNetwork.create(FireflyId.of(FireflyVertex.class, kr.key.userKey.toLong()), graph)
                                .records()
                                .forEachRemaining(subKr -> {
                                    traversalCache.insert(subKr.key, subKr.record);
                                });
                    });
        };
        return Optional.of(primeCacheOperation);
    }

}
