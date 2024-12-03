package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PartitionIterator implements CloseableIterator<Optional<CloseableIterator<FireflyVertex>>> {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionIterator.class);
    private final BlockingQueue<PageFetcher.Page> pageQueue;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final FireflyGraph graph;

    public static final class Builder {
        List<FireflyVertex> vertices = null;
        FireflyGraph graph;
        List<HasContainer> filters = new ArrayList<>();

        private Builder(final FireflyGraph graph) {
            this.graph = graph;

        }

        public Builder filters(final HasContainer... filters) {
            Collections.addAll(this.filters, filters);
            return this;
        }

        public Builder filters(final List<HasContainer> filters) {
            if (null != filters)
                this.filters.addAll(filters);
            return this;
        }

        public Builder filters(final GraphFilter filter) {
            return this.filters(Optional.ofNullable(filter.getVertexFilter()).map(t -> t.getSteps().stream()
                    .flatMap(s -> ((HasStep<Vertex>) s).getHasContainers().stream())
                    .collect(Collectors.toList())).orElse(null));
        }

        public Builder containers(final List<HasContainer> containers) {
            if (null != containers)
                this.filters.addAll(containers);
            return this;
        }

        public Builder vertices(final List<FireflyVertex> vertices) {
            this.vertices = vertices;
            return this;
        }

        public Builder partitionSize(final int pageSize) {
            this.graph.configuration().setProperty(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, pageSize);
            return this;
        }

        public Builder queueSize(final int queueSize) {
            this.graph.configuration().setProperty(ConfigurationHelper.Keys.PAGINATION_PAGE_QUEUE_SIZE, queueSize);
            return this;
        }

        public Builder maxWait(final int maxWait) {
            this.graph.configuration().setProperty(ConfigurationHelper.Keys.PAGINATION_PAGE_MAX_WAIT, maxWait);
            return this;
        }

        public PartitionIterator create() {
            return new PartitionIterator(this);
        }
    }

    public static Builder build(final FireflyGraph graph) {
        return new Builder(graph);
    }

    private PartitionIterator(final Builder builder) {
        this.graph = builder.graph;
        if (builder.vertices == null) {
            // If the last step did not wire through vertices, we need to run a scan / sindex using filters.
            this.pageQueue = graph.graphQuery.partitionVertexIdPages(builder.filters, graph.settings().evaluationTimeout);
        } else {
            // The last step wired through vertices, we can partition these and execute.
            this.pageQueue = graph.graphQuery.partitionVertices(builder.vertices);
        }
    }

    public boolean hasNext() {
        return !this.shutdown.get();
    }

    public Optional<CloseableIterator<FireflyVertex>> next() {
        return this.getPage(pageQueue, shutdown).map(p -> {
            if (p instanceof PageFetcher.VertexPage) {
                // If it's a vertex page we need to pass through the iterator.
                final PageFetcher.VertexPage vp = (PageFetcher.VertexPage) p;
                return vp.vertices;
            } else {
                // Running transform from scan / sindex.
                return FireflyCloseableIteratorUtils.map(p.keyRecords, graph::vertexFromRecord);
            }
        });
    }

    public void close() {
        // do nothing
    }

    private Optional<PageFetcher.Page> getPage(final BlockingQueue<PageFetcher.Page> pageQueue, final AtomicBoolean shutdown) {
        synchronized (this) {
            try {
                if (shutdown.get()) {
                    return Optional.empty();
                }
                final PageFetcher.Page page = pageQueue.take();

                if (page instanceof PageFetcher.ErrorPage) {
                    // ERROR
                    final PageFetcher.ErrorPage errorPage = (PageFetcher.ErrorPage) page;
                    LOG.error("ERROR: " + errorPage.errorMessage, errorPage.exception);
                    shutdown.set(true);
                    return Optional.empty();
                }
                if (page instanceof PageFetcher.PoisonPill) {
                    shutdown.set(true);
                    return Optional.empty();
                }
                return Optional.of(page);
            } catch (InterruptedException e) {
                shutdown.set(true);
                return Optional.empty();
            }
        }
    }

}



