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
