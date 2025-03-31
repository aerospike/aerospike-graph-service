package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.helper.AttachmentHelper;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.process.TraversalProgram;
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

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedMemory implements Memory.Admin, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedMemory.class);

    public final Map<String, MemoryComputeKey> memoryComputeKeys = new HashMap<>();
    private final Map<String, AccumulatorV2<DistributedMemoryEntry, DistributedMemoryEntry>> sparkMemory = new HashMap<>();
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);
    private Broadcast<Map<String, Object>> broadcast;
    private boolean inExecute = false;
    private transient FireflyGraph graph;

    public DistributedMemory(final VertexProgram<?> vertexProgram, final Set<MapReduce> mapReducers, final JavaSparkContext sparkContext) {
        TraversalProgram program = (TraversalProgram) vertexProgram;
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
            if (memoryComputeKey.getKey().endsWith("-accumulator")) {
                final TraversalMatrix tm = program.getTraversalMatrix();
                final RangeGlobalStep step = (RangeGlobalStep) tm.getStepById(memoryComputeKey.getKey().substring(0, memoryComputeKey.getKey().length() - "-accumulator".length()));
                ((FireflyGraph) program.getTraversal().get().getGraph().get()).getBaseGraph().setLimitBin("limit", step.getHighRange());
            }
            JavaSparkContext.toSparkContext(sparkContext).register(accumulator, memoryComputeKey.getKey());
            this.sparkMemory.put(memoryComputeKey.getKey(), accumulator);
        }
    }

    public void setGraph(final FireflyGraph graph) {
        this.graph = graph;
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
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration.set(iteration);
    }

    @Override
    public int getIteration() {
        return this.iteration.get();
    }

    @Override
    public void setRuntime(final long runTime) {
        this.runtime.set(runTime);
    }

    @Override
    public long getRuntime() {
        return this.runtime.get();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        if (!this.memoryComputeKeys.containsKey(key))
            throw Memory.Exceptions.memoryDoesNotExist(key);
        if (this.inExecute && !this.memoryComputeKeys.get(key).isBroadcast())
            throw Memory.Exceptions.memoryDoesNotExist(key);
        if (!this.sparkMemory.containsKey(key))
            throw Memory.Exceptions.memoryDoesNotExist(key);

        if (key.endsWith("-accumulator")) {
            return (R) graph.getBaseGraph().getLimitBin("limit");
        }

        final DistributedMemoryEntry<R> r = (DistributedMemoryEntry<R>) (this.inExecute ? this.broadcast.value().get(key) : this.sparkMemory.get(key).value());
        if (null == r || r.isEmpty()) {
            // gremlin.traversalVertexProgram.completedBarriers
            //
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
        if (key.endsWith("-accumulator")) {
            graph.getBaseGraph().addLimitBin("limit", (long) value);
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
        if (key.endsWith("-accumulator")) {
            throw new IllegalStateException("Cant set aerospike compute key.");
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
