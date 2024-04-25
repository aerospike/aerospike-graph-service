package com.aerospike.firefly.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FireflyWorkerMemory implements Memory.Admin {

    private final FireflyMemory mainMemory;
    private final Map<String, Object> workerMemory = new HashMap<>();
    private final Map<String, BinaryOperator<Object>> reducers = new HashMap<>();

    public FireflyWorkerMemory(final FireflyMemory mainMemory) {
        this.mainMemory = mainMemory;
        for (final MemoryComputeKey key : this.mainMemory.memoryKeys.values()) {
            this.reducers.put(key.getKey(), key.clone().getReducer());
        }
    }

    @Override
    public Set<String> keys() {
        return this.mainMemory.keys();
    }

    @Override
    public void incrIteration() {
        this.mainMemory.incrIteration();
    }

    @Override
    public void setIteration(final int iteration) {
        this.mainMemory.setIteration(iteration);
    }

    @Override
    public int getIteration() {
        return this.mainMemory.getIteration();
    }

    @Override
    public void setRuntime(final long runTime) {
        this.mainMemory.setRuntime(runTime);
    }

    @Override
    public long getRuntime() {
        return this.mainMemory.getRuntime();
    }

    @Override
    public boolean isInitialIteration() {
        return this.mainMemory.isInitialIteration();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        return this.mainMemory.get(key);
    }

    @Override
    public void set(final String key, final Object value) {
        this.mainMemory.set(key, value);
    }

    @Override
    public void add(final String key, final Object value) {
        this.mainMemory.checkKeyValue(key, value);
        final Object v = this.workerMemory.get(key);
        this.workerMemory.put(key, null == v ? value : this.reducers.get(key).apply(v, value));
    }

    @Override
    public String toString() {
        return this.mainMemory.toString();
    }

    protected void complete() {
        for (final Map.Entry<String, Object> entry : this.workerMemory.entrySet()) {
            this.mainMemory.add(entry.getKey(), entry.getValue());
        }
        this.workerMemory.clear();
    }
}