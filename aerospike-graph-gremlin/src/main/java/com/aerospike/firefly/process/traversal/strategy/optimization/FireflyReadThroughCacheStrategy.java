package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.ReadThroughRecordCache;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

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
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
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

        final AerospikeConnection db = ((FireflyGraph) (graphOptional.get())).getBaseGraph();
        final UUID uuid = UUID.randomUUID();

        if (db.transactionCache.get() != null) {
            final FireflyCache cache = db.transactionCache.get();
            db.lastQueryHitCount = cache.getHitCount();
            db.lastQueryMissCount = cache.getMissCount();
            db.transactionCache.get().invalidateAll();
        } else {
            db.transactionCache.set(new ReadThroughRecordCache(db, uuid));
        }
        if (db.emptyPropsTransactionCache.get() != null) {
            final FireflyCache cache = db.emptyPropsTransactionCache.get();
            db.lastQueryHitCount = cache.getHitCount() + db.lastQueryHitCount;
            db.lastQueryMissCount = cache.getMissCount() + db.lastQueryMissCount;
            db.emptyPropsTransactionCache.get().invalidateAll();
        } else {
            db.emptyPropsTransactionCache.set(new ReadThroughRecordCache(db, uuid));
        }
    }
}
