package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.ReadThroughRecordCache;
import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyReadThroughCacheStrategy extends FireflyStrategyBase {
    /**
     * Default constructor for FireflyReadThroughCacheStrategy.
     */
    public FireflyReadThroughCacheStrategy() {
    }

    @Override
    protected String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_READ_THROUGH_CACHE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.isRoot()) {
            return;
        }

        final Optional<Graph> graphOptional = traversal.getGraph();
        if (graphOptional.isEmpty()) {
            return;
        }
        if (!(graphOptional.get() instanceof FireflyGraph)) {
            return;
        }
        if (!(traversal.getStartStep() instanceof GraphStep)) {
            return;
        }
        final AerospikeConnection db = ((FireflyGraph) (graphOptional.get())).getBaseGraph();
        final UUID uuid = UUID.randomUUID();

        // Now that we know this is a supported traversal.
        // Set the traversal thread-local reference.
        final FireflyCache cache = new ReadThroughRecordCache(db, uuid);
        db.transactionCache.set(cache);
        final FireflyCache emptyPropsCache = new ReadThroughRecordCache(db, uuid);
        db.emptyPropsTransactionCache.set(emptyPropsCache);

        // Tack on the step that will remove the cache when it's finished.
        final FireflyCacheGCStep gcStep = new FireflyCacheGCStep(traversal, new HashSet<>(traversal.getEndStep().getLabels()));

        // Profile must be last if it exists.
        if (TraversalHelper.hasStepOfClass(ProfileSideEffectStep.class, traversal)) {
            // Add before profile step.
            traversal.addStep(traversal.getSteps().size() - 2, gcStep);
        } else {
            // No profile step, add to end.
            traversal.addStep(gcStep);
        }
    }
}
