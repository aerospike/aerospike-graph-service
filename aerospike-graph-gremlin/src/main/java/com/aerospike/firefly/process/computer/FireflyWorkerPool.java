package com.aerospike.firefly.process.computer;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MapReducePool;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramPool;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.TriConsumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FireflyWorkerPool implements AutoCloseable {

    private static final BasicThreadFactory THREAD_FACTORY_WORKER = new BasicThreadFactory.Builder().namingPattern("firefly-worker-%d").build();


    private final int numberOfWorkers;
    private final ExecutorService workerPool;
    private final CompletionService<Object> completionService;

    private VertexProgramPool vertexProgramPool;
    private MapReducePool mapReducePool;
    private final Queue<FireflyWorkerMemory> workerMemoryPool = new ConcurrentLinkedQueue<>();
    private final List<List<Vertex>> workerVertices = new ArrayList<>();

    public FireflyWorkerPool(final FireflyGraph graph, final FireflyMemory memory, final int numberOfWorkers) {
        this.numberOfWorkers = numberOfWorkers;
        this.workerPool = Executors.newFixedThreadPool(numberOfWorkers, THREAD_FACTORY_WORKER);
        this.completionService = new ExecutorCompletionService<>(this.workerPool);
        for (int i = 0; i < this.numberOfWorkers; i++) {
            this.workerMemoryPool.add(new FireflyWorkerMemory(memory));
            this.workerVertices.add(new ArrayList<>());
        }
        long batchSize = 5; // ((long) graph.traversal().<Map<String,Object>>call("summary").next().get("Total vertex count")) / this.numberOfWorkers;
        //if(true)
        //throw new IllegalStateException("the number of workers is " + this.numberOfWorkers + " and the batch size is " + batchSize);
        if (0 == batchSize)
            batchSize = 1;
        int counter = 0;
        int index = 0;

        List<Vertex> currentWorkerVertices = this.workerVertices.get(index);
        final Iterator<Vertex> iterator = graph.vertices();
        while (iterator.hasNext()) {
            final Vertex vertex = iterator.next();
            if (counter++ < batchSize || index == this.workerVertices.size() - 1) {
                currentWorkerVertices.add(vertex);
            } else {
                currentWorkerVertices = this.workerVertices.get(++index);
                currentWorkerVertices.add(vertex);
                counter = 1;
            }
        }
    }

    public void setVertexProgram(final VertexProgram vertexProgram) {
        this.vertexProgramPool = new VertexProgramPool(vertexProgram, this.numberOfWorkers);
    }

    public void setMapReduce(final MapReduce mapReduce) {
        this.mapReducePool = new MapReducePool(mapReduce, this.numberOfWorkers);
    }

    public void executeVertexProgram(final TriConsumer<Iterator<Vertex>, VertexProgram, FireflyWorkerMemory> worker) throws InterruptedException {
        for (int i = 0; i < this.numberOfWorkers; i++) {
            final int index = i;
            this.completionService.submit(() -> {
                final VertexProgram vp = this.vertexProgramPool.take();
                final FireflyWorkerMemory workerMemory = this.workerMemoryPool.poll();
                final List<Vertex> vertices = this.workerVertices.get(index);
                worker.accept(vertices.iterator(), vp, workerMemory);
                this.vertexProgramPool.offer(vp);
                this.workerMemoryPool.offer(workerMemory);
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