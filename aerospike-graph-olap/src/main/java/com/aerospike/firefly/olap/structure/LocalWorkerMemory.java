package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.helper.AttachmentHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;

public class LocalWorkerMemory implements Memory.Admin {

    private final DistributedMemory mainMemory;
    private final FireflyGraph graph;
    private final Map<String, Object> workerMemory = new HashMap<>();
    private final Map<String, BinaryOperator<Object>> reducers = new HashMap<>();
    // cache for attached objects from main memory
    private final Map<String, Object> readCache = new HashMap<>();

    public LocalWorkerMemory(final DistributedMemory mainMemory, final FireflyGraph graph) {
        this.mainMemory = mainMemory;
        this.graph = graph;
        for (final MemoryComputeKey key : this.mainMemory.memoryComputeKeys.values()) {
            this.reducers.put(key.getKey(), key.clone().getReducer());
        }
        this.mainMemory.setInExecute(true);
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
        if (readCache.containsKey(key)) {
            return (R) readCache.get(key);
        }

        R result = this.mainMemory.get(key);
        // for select() step
        if (result instanceof Collection) {
            // special case for limited implementation's of List, like Arrays.ArrayList (produced by Arrays.asList())
            // or ImmutableCollections.List12 (produced by List.of())
            if (result instanceof List && !(result instanceof ArrayList)) {
                result = (R) new ArrayList<>((List) result);
            } else if (result instanceof BulkSet) {
                final BulkSet tmp = new BulkSet();
                tmp.addAll((BulkSet) result);
                result = (R) tmp;
            }
            AttachmentHelper.bulkAttach(graph, (Collection) result);
        }

        readCache.put(key, result);

        return result;
    }

    @Override
    public void set(final String key, final Object value) {
        this.mainMemory.set(key, value);
    }

    @Override
    public void add(final String key, final Object value) {
        if (mainMemory.memoryComputeKeys.containsKey(String.format("%s-accumulator", key))) {
            if (value instanceof Collection) {
                final Collection<Traverser> traversers = (Collection) value;
                long bulkCount = 0;
                for (final Traverser traverser : traversers) {
                    bulkCount += traverser.bulk();
                }
                mainMemory.add(String.format("%s-accumulator", key), -bulkCount);
            }
        }
        this.mainMemory.checkKeyValue(key, value);
        this.workerMemory.compute(key, (k, v) -> null == v ? value : this.reducers.get(key).apply(v, value));
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
        this.mainMemory.setInExecute(false);
    }
}
