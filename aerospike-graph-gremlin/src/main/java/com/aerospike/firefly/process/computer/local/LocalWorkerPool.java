package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.io.aerospike.query.paged.PartitionIterator;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MapReducePool;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramPool;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    public List<Element> executeVertexProgram(
            final LocalGraphComputer.ExecuteVertexProgram executeVertexProgram,
            boolean isFirstStep,
            final List<Element> elements,
            final GraphFilter filter,
            final List<HasContainer> hasContainers) throws InterruptedException {
        final long vertexCount = (long) ((Map<Object, Object>) this.graph.traversal().call("aerospike.graph.admin.metadata.summary").next()).get("Total vertex count");
        final int partitionSize = Math.max(
                ((int) Math.ceil((double) vertexCount / (double) numberOfWorkers)),
                ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, this.graph.configuration()));
        LOG.debug("VERTEX PROGRAM STAGE PARTITION CONFIGURATION" +
                        "\n\tVertices in summary metadata: {}" +
                        "\n\tComputed partition size: {}" +
                        "\n\tPartition queue size: {}" +
                        "\n\tPartition max wait: {}\n",
                vertexCount,
                partitionSize,
                ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_QUEUE_SIZE, this.graph.configuration()),
                ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_MAX_WAIT, this.graph.configuration()));
        final AtomicLong counter = new AtomicLong(0);
        final PartitionIterator.Builder builder = PartitionIterator.build(this.graph).partitionSize(partitionSize);
        if (isFirstStep) {
            // If first step we need to use has containers.
            builder.containers(hasContainers);
        } else {
            if (elements.isEmpty()) {
                // If no elements from previous loop and not first step, we need to use global filters.
                builder.filters(filter);
            } else {
                // Elements from previous loop.
                builder.vertices((List) elements);
            }
        }
        final List<Element> results = Collections.synchronizedList(new ArrayList());
        try (final PartitionIterator partitions = builder.create()) {
            for (int i = 0; i < this.numberOfWorkers; i++) {
                final int index = i;
                this.completionService.submit(() -> {
                    long count = 0;
                    final VertexProgram<?> vp = this.vertexProgramPool.take();
                    final LocalWorkerMemory workerMemory = this.workerMemoryPool.poll();
                    while (true) {
                        final Optional<CloseableIterator<FireflyVertex>> option = partitions.next();
                        List<FireflyVertex> batch;
                        if (option.isPresent()) {
                            try {
                                batch = IteratorUtils.toList(option.get());
                                count += batch.size();
                                final List<Object> batchIds = batch.stream().map(e->e.id()).collect(Collectors.toList());
                                if (!batch.isEmpty()) {
                                    final List<Element> output = executeVertexProgram.execute(null, workerMemory,
                                            // id -> ((Integer)id) % numberOfWorkers == index);
                                            id -> batchIds.contains(id));
                                    if (output != null) {
                                        results.addAll(output);
                                        counter.addAndGet(output.size());
                                    }
                                }
                                LOG.info("Worker {} processed {} vertices, total={}", index, count, counter.get());
                            } catch (final Exception e) {
                                LOG.error("Worker {} failed on {} vertex of partition", index, count, e);
                            }
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
        return results;
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

    public void closeNow() {
        this.workerPool.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        this.workerPool.shutdown();
    }
}
