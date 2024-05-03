package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.io.aerospike.query.paged.PartitionIterator;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MapReducePool;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramPool;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalWorkerPool implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalWorkerPool.class);
    private static final BasicThreadFactory THREAD_FACTORY_WORKER = new BasicThreadFactory.Builder().namingPattern("firefly-worker-%d").build();

    private final int numberOfWorkers;
    private final ExecutorService workerPool;
    private final CompletionService<Object> completionService;

    private VertexProgramPool vertexProgramPool;
    private MapReducePool mapReducePool;
    private final Queue<LocalWorkerMemory> workerMemoryPool = new ConcurrentLinkedQueue<>();
    private final FireflyGraph graph;

    public LocalWorkerPool(final FireflyGraph graph, final LocalMemory memory, final int numberOfWorkers) {
        this.graph = graph;
        this.numberOfWorkers = numberOfWorkers;
        this.workerPool = Executors.newFixedThreadPool(numberOfWorkers, THREAD_FACTORY_WORKER);
        this.completionService = new ExecutorCompletionService<>(this.workerPool);
        for (int i = 0; i < this.numberOfWorkers; i++) {
            this.workerMemoryPool.add(new LocalWorkerMemory(memory));
        }
    }

    public void setVertexProgram(final VertexProgram vertexProgram) {
        this.vertexProgramPool = new VertexProgramPool(vertexProgram, this.numberOfWorkers);
    }

    public void setMapReduce(final MapReduce mapReduce) {
        this.mapReducePool = new MapReducePool(mapReduce, this.numberOfWorkers);
    }

    public void executeVertexProgram(final TriFunction<Iterator<FireflyVertex>, VertexProgram, LocalWorkerMemory, Long> worker) throws InterruptedException {
        long vertexCount = 5000; // TODO: a fast way to compute graph size
        int partitionSize = Math.max(128, (int) Math.round((double) vertexCount / (double) numberOfWorkers));
        try (final PartitionIterator partitions = PartitionIterator.build(this.graph).partitionSize(partitionSize).create()) {
            for (int i = 0; i < this.numberOfWorkers; i++) {
                final int index = i;
                this.completionService.submit(() -> {
                    long count;
                    final VertexProgram vp = this.vertexProgramPool.take();
                    final LocalWorkerMemory workerMemory = this.workerMemoryPool.poll();
                    while (true) {
                        Optional<CloseableIterator<FireflyVertex>> option = partitions.next();
                        if (option.isPresent()) {
                            LOG.warn("Worker {} retrieved new vertex page workload", index);
                            count = worker.apply(option.get(), vp, workerMemory);
                            LOG.warn("Worker {} processed {} vertices", index, count);
                        } else {
                            break;
                        }
                    }
                    this.vertexProgramPool.offer(vp);
                    this.workerMemoryPool.offer(workerMemory);
                    return null;
                });
            }
        }
        for (int i = 0; i < this.numberOfWorkers; i++) {
            try {
                this.completionService.take().get();
            } catch (InterruptedException ie) {
                throw ie;
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    public void executeMapReduce(final Consumer<MapReduce> worker) throws InterruptedException {
        for (int i = 0; i < this.numberOfWorkers; i++) {
            this.completionService.submit(() -> {
                final MapReduce mr = this.mapReducePool.take();
                worker.accept(mr);
                this.mapReducePool.offer(mr);
                return null;
            });
        }
        for (int i = 0; i < this.numberOfWorkers; i++) {
            try {
                this.completionService.take().get();
            } catch (InterruptedException ie) {
                throw ie;
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    public void closeNow() throws Exception {
        this.workerPool.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        this.workerPool.shutdown();
    }
}