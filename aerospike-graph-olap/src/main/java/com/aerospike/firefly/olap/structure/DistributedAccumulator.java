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

package com.aerospike.firefly.olap.structure;

import org.apache.spark.util.AccumulatorV2;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;

import java.io.Serializable;

/**
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedAccumulator<A> extends AccumulatorV2<DistributedMemoryEntry<A>, DistributedMemoryEntry<A>> implements Serializable {

    private final MemoryComputeKey<A> memoryComputeKey;
    private DistributedMemoryEntry<A> value;

    DistributedAccumulator(final MemoryComputeKey<A> memoryComputeKey) {
        this(memoryComputeKey, DistributedMemoryEntry.empty());
    }

    private DistributedAccumulator(final MemoryComputeKey<A> memoryComputeKey, final DistributedMemoryEntry<A> initial) {
        this.memoryComputeKey = memoryComputeKey;
        this.value = initial;
    }

    @Override
    public boolean isZero() {
        return DistributedMemoryEntry.empty().equals(value);
    }

    @Override
    public AccumulatorV2<DistributedMemoryEntry<A>, DistributedMemoryEntry<A>> copy() {
        return new DistributedAccumulator<>(this.memoryComputeKey, DistributedMemoryEntry.empty());
    }

    @Override
    public void reset() {
        this.value = DistributedMemoryEntry.empty();
    }

    @Override
    public void add(final DistributedMemoryEntry<A> v) {
        if (this.value.isEmpty())
            this.value = v;
        else if (!v.isEmpty())
            this.value = new DistributedMemoryEntry<>(this.memoryComputeKey.getReducer().apply(value.get(), v.get()));
    }

    @Override
    public void merge(final AccumulatorV2<DistributedMemoryEntry<A>, DistributedMemoryEntry<A>> other) {
        this.add(other.value());
    }

    @Override
    public DistributedMemoryEntry<A> value() {
        return this.value;
    }
}
