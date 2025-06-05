package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.helper.AttachmentHelper;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.process.FireflyProgram;
import com.aerospike.firefly.olap.process.TraversalProgram;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.IndexedTraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.olap.structure.AerospikeComputeKey.isAccumulator;
import static com.aerospike.firefly.olap.structure.AerospikeComputeKey.isDouble;
import static com.aerospike.firefly.olap.structure.AerospikeComputeKey.isLong;
import static com.aerospike.firefly.olap.structure.AerospikeComputeKey.isVersioned;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedMemory implements Memory.Admin, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedMemory.class);

    public final Map<String, MemoryComputeKey> memoryComputeKeys = new HashMap<>();
    private final Map<String, AccumulatorV2<DistributedMemoryEntry, DistributedMemoryEntry>> sparkMemory = new HashMap<>();
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0L);
    private Broadcast<Map<String, Object>> broadcast;
    private boolean inExecute = false;
    private transient DistributedAerospikeConnection db;

    public DistributedMemory(final VertexProgram<?> vertexProgram, final Set<MapReduce> mapReducers, final JavaSparkContext sparkContext) {
        FireflyProgram program = (FireflyProgram) vertexProgram;
        if (null != vertexProgram) {
            for (final MemoryComputeKey key : vertexProgram.getMemoryComputeKeys()) {
                this.memoryComputeKeys.put(key.getKey(), key);
            }
        }
        for (final MapReduce mapReduce : mapReducers) {
            this.memoryComputeKeys.put(mapReduce.getMemoryKey(), MemoryComputeKey.of(mapReduce.getMemoryKey(), Operator.assign, false, false));
        }
        this.broadcast = sparkContext.broadcast(Collections.emptyMap());
        for (final MemoryComputeKey memoryComputeKey : this.memoryComputeKeys.values()) {
            final AccumulatorV2<DistributedMemoryEntry, DistributedMemoryEntry> accumulator = new DistributedAccumulator<>(memoryComputeKey);
            if (program instanceof TraversalProgram && isAccumulator(memoryComputeKey.getKey())) {
                final TraversalMatrix tm = ((TraversalProgram) program).getTraversalMatrix();
                final RangeGlobalStep step = (RangeGlobalStep) tm.getStepById(memoryComputeKey.getKey().substring(0, memoryComputeKey.getKey().length() - "-accumulator".length()));
                this.db = new DistributedAerospikeConnection(((FireflyGraph) program.getTraversal().get().getGraph().get()));
                this.db.setAccumulator(memoryComputeKey.getKey(), step.getHighRange());
            }
            JavaSparkContext.toSparkContext(sparkContext).register(accumulator, memoryComputeKey.getKey());
            this.sparkMemory.put(memoryComputeKey.getKey(), accumulator);
        }
    }

    public void setGraph(final FireflyGraph graph) {
        this.db = new DistributedAerospikeConnection(graph);
    }

    @Override
    public Set<String> keys() {
        if (this.inExecute)
            return this.broadcast.getValue().keySet();
        else {
            final Set<String> trueKeys = new HashSet<>();
            this.sparkMemory.forEach((key, value) -> {
                if (!value.value().isEmpty())
                    trueKeys.add(key);
            });
            return Collections.unmodifiableSet(trueKeys);
        }
    }

    @Override
    public void incrIteration() {
        this.iteration.getAndIncrement();

        // copy accumulator values for new iteration
        for (final String key : this.memoryComputeKeys.keySet()) {
            copyFromPreviousIteration(key);
        }
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration.set(iteration);
    }

    @Override
    public int getIteration() {
        return this.iteration.get();
    }

    // used to copy value from previous iteration
    public String previousKey(final String key) {
        return isVersioned(key) ? key + (this.iteration.get() - 1) : key + (this.iteration.get() % 2 == 0 ? 1 : 0);
    }

    public String readKey(final String key) {
        // if in execute then read previous value
        final int n = this.iteration.get() - (inExecute ? 1 : 0);
        return isVersioned(key) ? key + n : key + (n % 2 == 0 ? 0 : 1);
    }

    public String writeKey(final String key) {
        return isVersioned(key) ? key + this.iteration.get() : key + (this.iteration.get() % 2 == 0 ? 0 : 1);
    }

    @Override
    public void setRuntime(final long runTime) {
        this.runtime.set(runTime);
    }

    @Override
    public long getRuntime() {
        return this.runtime.get();
    }

    private void copyFromPreviousIteration(final String key) {
        if (isLong(key)) {
            final Long value = db.getAccumulatorLong(previousKey(key));
            db.setAccumulator(writeKey(key), value);
        } else if (isDouble(key)) {
            final Double value = db.getAccumulatorDouble(previousKey(key));
            db.setAccumulator(writeKey(key), value);
        }
    }

    public Map<String, Object> getBroadcastValues() {
        final Map<String, Object> result = new HashMap<>();
        for (final MemoryComputeKey key : this.memoryComputeKeys.values()) {
            if (key.isBroadcast()) {
                final DistributedMemoryEntry entry = this.sparkMemory.get(key.getKey()).value();
                if (entry != null && !entry.isEmpty())
                    result.put(key.getKey(), entry.get());
            }
        }
        return result;
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        if (!this.memoryComputeKeys.containsKey(key))
            throw Memory.Exceptions.memoryDoesNotExist(key);
        if (this.inExecute && !this.memoryComputeKeys.get(key).isBroadcast())
            throw Memory.Exceptions.memoryDoesNotExist(key);
        if (!this.sparkMemory.containsKey(key))
            throw Memory.Exceptions.memoryDoesNotExist(key);

        if (isAccumulator(key)) {
            return (R) db.getAccumulatorLong(key);
        }
        if (isLong(key)) {
            return (R) db.getAccumulatorLong(readKey(key));
        }
        if (isDouble(key)) {
            return (R) db.getAccumulatorDouble(readKey(key));
        }

        final DistributedMemoryEntry<R> r = (DistributedMemoryEntry<R>) (this.inExecute ? this.broadcast.value().get(key) : this.sparkMemory.get(key).value());
        if (null == r || r.isEmpty()) {
            throw Memory.Exceptions.memoryDoesNotExist(key);
        } else {
            final R rr = r.get();
            if (rr instanceof DistributedIndexedTraverserSet) {
                return (R) new IndexedTraverserSet<>(((DistributedIndexedTraverserSet) rr).indexingFunction);
            } else {
                return rr;
            }
        }
    }

    @Override
    public void add(final String key, final Object value) {
        checkKeyValue(key, value);
        if (isAccumulator(key)) {
            db.addAccumulator(key, (long) value);
            return;
        }
        if (isLong(key)) {
            db.addAccumulator(writeKey(key), (Long) value);
            return;
        }
        if (isDouble(key)) {
            db.addAccumulator(writeKey(key), (Double) value);
            return;
        }

        final Object detachedValue = AttachmentHelper.detach(value, true); // !key.equals(HALTED_TRAVERSERS)
        if (this.inExecute) {
            if (key.endsWith(")"))
                TaskLogger.logDebuggingMessage("Adding " + key + " to broadcast memory: " + detachedValue + " (from " + value + ").", LOGGER);
            this.sparkMemory.get(key).add(new DistributedMemoryEntry<>(detachedValue));
        } else
            throw Memory.Exceptions.memoryAddOnlyDuringVertexProgramExecute(key);
    }

    @Override
    public void set(final String key, Object value) {
        if (isAccumulator(key)) {
            db.setAccumulator(key, (Long) value);
            return;
        }
        if (isLong(key)) {
            db.setAccumulator(writeKey(key), (Long) value);
            return;
        }
        if (isDouble(key)) {
            db.setAccumulator(writeKey(key), (Double) value);
            return;
        }

        if (value instanceof IndexedTraverserSet.VertexIndexedTraverserSet) {
            value = new DistributedIndexedTraverserSet<>((IndexedTraverserSet.VertexIndexedTraverserSet) value);
        }
        checkKeyValue(key, value);
        if (this.inExecute)
            throw Memory.Exceptions.memorySetOnlyDuringVertexProgramSetUpAndTerminate(key);
        else {
            this.sparkMemory.get(key).reset();
            this.sparkMemory.get(key).add(new DistributedMemoryEntry<>(value));
        }
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    protected void complete() {
        this.memoryComputeKeys.values().stream().filter(MemoryComputeKey::isTransient).forEach(memoryComputeKey -> this.sparkMemory.remove(memoryComputeKey.getKey()));
    }

    public void setInExecute(final boolean inExecute) {
        this.inExecute = inExecute;
    }

    protected void broadcastMemory(final JavaSparkContext sparkContext) {
        this.broadcast.destroy(true); // do we need to block?
        final Map<String, Object> toBroadcast = new HashMap<>();
        this.sparkMemory.forEach((key, object) -> {
            if (!object.value().isEmpty() && this.memoryComputeKeys.get(key).isBroadcast())
                toBroadcast.put(key, object.value());
        });
        this.broadcast = sparkContext.broadcast(toBroadcast);
    }

    protected void checkKeyValue(final String key, final Object value) {
        if (!this.memoryComputeKeys.containsKey(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
    }
}
