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

package com.aerospike.firefly.process.traversal.strategy.options;

import com.aerospike.firefly.io.aerospike.AerospikeConnectionConfig;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyStrategyBase;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Optional;

import static com.aerospike.firefly.util.config.ConfigurationHelper.getTraversalOptionBoolean;

public class FireflyTraversalOptionsStrategy extends FireflyStrategyBase {
    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.isRoot()) {
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            final AerospikeConnectionConfig config = graph.getBaseGraph().getConfig();
            final Optional<Boolean> allowScanOption = getTraversalOptionBoolean(ConfigurationHelper.TraversalOptions.SCAN_QUERY_ENABLED, traversal, true);
            if (allowScanOption.isPresent()) {
                config.setAllowScanTraversalOption(allowScanOption.get());
            } else {
                config.clearAllowScanTraversalOption();
            }
        }
    }
}
