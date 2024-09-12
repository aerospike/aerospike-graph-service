package com.aerospike.firefly.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.util.Iterator;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.util.Tokens.ARGS_EVAL_TIMEOUT;

public final class TimeoutHelper {
    private TimeoutHelper() {
    }

    /**
     * Calculate evaluation timeout from traversal, use default Graph settings as backup.
     *
     * @return timeout in milliseconds
     */
    public static long calculate(final Traversal.Admin traversal) {
        long timeout = new Settings().evaluationTimeout;
        if (traversal.getGraph().isPresent()) {
            timeout = ((FireflyGraph) traversal.getGraph().get()).settings().evaluationTimeout;
        }

        final Iterator<OptionsStrategy> itty = BytecodeHelper.findStrategies(traversal.getBytecode(), OptionsStrategy.class);
        while (itty.hasNext()) {
            final OptionsStrategy optionsStrategy = itty.next();
            final Map<String, Object> options = optionsStrategy.getOptions();
            if (options.containsKey(ARGS_EVAL_TIMEOUT)) {
                timeout = ((Number) options.get(ARGS_EVAL_TIMEOUT)).longValue();
            }
        }

        return timeout;
    }
}
