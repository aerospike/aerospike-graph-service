package com.aerospike.firefly.olap.structure;

import org.apache.spark.util.AccumulatorV2;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;

import java.io.Serializable;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
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
