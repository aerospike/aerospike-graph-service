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

package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

public class FireflyDropStep extends AbstractStep {
    private static final Logger LOGGER = LoggerFactory.getLogger(FireflyDropStep.class);
    private final AtomicBoolean isDone;

    public FireflyDropStep(final Traversal.Admin traversal) {
        super(traversal);
        this.isDone = new AtomicBoolean(false);
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        if (!isDone.getAndSet(true)) {
            final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();
            graph.getBaseGraph().dropDatabase(graph, false);
            if (graph.getBaseGraph().getConfig().isAuditLogEnabled) {
                LOGGER.info("[{}] Dropped entire database.", graph.getUser());
            }
        }
        throw FastNoSuchElementException.instance();
    }
}
