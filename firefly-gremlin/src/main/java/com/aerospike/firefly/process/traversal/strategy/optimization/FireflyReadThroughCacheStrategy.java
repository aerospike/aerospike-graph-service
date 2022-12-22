package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.impl.ReadThroughCache;
import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.RequirementsStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyReadThroughCacheStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final FireflyReadThroughCacheStrategy INSTANCE = new FireflyReadThroughCacheStrategy();

    private FireflyReadThroughCacheStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
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
        final AerospikeConnection db = ((FireflyGraph)(graphOptional.get())).getBaseGraph();
        final UUID uuid = UUID.randomUUID();

        // Now that we know this is a supported traversal.
        // Set the traversal thread-local reference.
        final FireflyCache cache = new ReadThroughCache(db, uuid);
        db.transactionCache.set(cache);

        // Tack on the step that will remove the cache when it's finished.
        final FireflyCacheGCStep gcStep = new FireflyCacheGCStep(traversal, cache, new HashSet<>(traversal.getEndStep().getLabels()));

        // Profile must be last if it exists.
        if (TraversalHelper.hasStepOfClass(ProfileSideEffectStep.class, traversal)) {
            // Add before profile step.
            traversal.addStep(traversal.getSteps().size() - 2, gcStep);
        } else {
            // No profile step, add to end.
            traversal.addStep(gcStep);
        }
    }

    public static FireflyReadThroughCacheStrategy instance() {
        return INSTANCE;
    }
}
