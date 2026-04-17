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

package com.aerospike.firefly.olap.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;

public class BatchJob {
    private TraverserSet starts;
    private final TraverserSet<Traverser.Admin> results;

    public BatchJob(final TraverserSet starts) {
        this.starts = starts;
        this.results = new TraverserSet<>();
    }

    public TraverserSet<Object> getStarts() {
        return starts;
    }

    public TraverserSet<Traverser.Admin> getResults() {
        return results;
    }

    public void addResult(final Traverser.Admin traverser) {
        this.results.add(traverser);
    }

    public void pass() {
        this.results.addAll(this.starts);
    }

    public void setStarts(final TraverserSet<Object> starts) {
        this.starts = starts;
    }

    public void clear() {
        this.starts.clear();
        this.results.clear();
    }

}
