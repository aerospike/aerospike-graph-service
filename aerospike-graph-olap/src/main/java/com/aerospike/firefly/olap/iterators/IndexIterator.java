package com.aerospike.firefly.olap.iterators;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.query.paged.PageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.PaginationIterator;
import com.aerospike.firefly.io.aerospike.query.paged.PartitionedSindexPageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.VertexQueryHelper;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.TimeoutHelper;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import static com.aerospike.firefly.olap.structure.DistributedWorkerExecutor.COUNT_COL;
import static com.aerospike.firefly.olap.structure.DistributedWorkerExecutor.START_COL;

public class IndexIterator implements CloseableIterator<Traverser> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexIterator.class);
    final FireflyGraph graph;
    final Filter filter;
    LinkedBlockingQueue<PageFetcher.Page> pageQueue;
    final List<Row> rows = new ArrayList<>();
    final List<HasContainer> hasContainers;
    final String startStep;
    final Codec codec;
    final Traversal traversal;
    final FireflyIndexMetadata.IndexInfo indexInfo;
    final TraverserGenerator tg;
    private final GraphStep graphStep;
    int rowCount = 0;
    PageFetcher<?> pageFetcher = null;
    PageFetcher.Page page = null;

    public IndexIterator(final FireflyGraph graph,
                         final GraphStep graphStep,
                         final HasContainer hasContainer,
                         final List<HasContainer> hasContainers,
                         final String startStep,
                         final Codec codec,
                         final FireflyIndexMetadata.IndexInfo indexInfo,
                         final Iterator<Row> iterator,
                         final Traversal traversal,
                         final TraverserGenerator tg) {
        this.graphStep = graphStep;
        this.tg = tg;
        this.indexInfo = indexInfo;
        this.graph = graph;
        this.filter = VertexQueryHelper.predicateToFilter(graph.getBaseGraph(), hasContainer.getPredicate(), indexInfo);
        while (iterator.hasNext()) {
            rows.add(iterator.next());
        }
        this.hasContainers = hasContainers;
        this.startStep = startStep;
        this.codec = codec;
        this.traversal = traversal;
        this.pageQueue = new LinkedBlockingQueue<>(graph.getBaseGraph().getConfig().paginationPageQueueSize);
        graph.logMessage("IndexIterator created with " + rows.size() + " rows.", LOGGER);
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (rows.isEmpty() || rows.size() == rowCount) {
                return false;
            }

            if (page != null) {
                if (page.keyRecords.hasNext()) {
                    return true;
                } else {
                    final PaginationIterator pi = (PaginationIterator) page.keyRecords;
                    pi.close();
                }
            }

            // Now current index.
            if (pageFetcher == null) {
                int attemptCount = 0;
                while (true) {
                    try {
                        final Row row = rows.get(rowCount);
                        final PartitionFilter partitionFilter = PartitionFilter.range(
                                row.getInt(row.fieldIndex(START_COL)),
                                row.getInt(row.fieldIndex(COUNT_COL)));


                        // TODO: Can omit some bins here.
                        final int evaluationTimeout = Long.valueOf(TimeoutHelper.calculate(traversal.asAdmin())).intValue();
                        final QueryPolicy policy = new QueryPolicy();
                        policy.setTimeout(evaluationTimeout);
                        attemptCount++;
                        pageFetcher = new PartitionedSindexPageFetcher<>(
                                graph,
                                policy,
                                indexInfo.setName,
                                graph.getBaseGraph().getNamespace(),
                                filter,
                                graph.getBaseGraph().getConfig().paginationPageSize,
                                graph::vertexFromRecord,
                                indexInfo.indexName,
                                partitionFilter,
                                pageQueue);

                        // Intentionally not using return value here.
                        pageFetcher.startQueryDirect();
                        break;
                    } catch (final Exception e) {
                        if (attemptCount > 10) {
                            throw new RuntimeException("Failed to run query after " + attemptCount + " attempts.", e);
                        } else if (e.getMessage().contains("Operation not allowed at this time")) {
                            TaskLogger.logDebuggingMessage("Sleeping.", LOGGER);
                            try {
                                Thread.sleep(1000L * (attemptCount + 1));
                            } catch (final InterruptedException e1) {
                                throw new RuntimeException(e1);
                            }
                        }
                    }
                }
            }

            try {
                page = pageQueue.take();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (page instanceof PageFetcher.ErrorPage) {
                final PageFetcher.ErrorPage errorPage = (PageFetcher.ErrorPage) page;
                throw new RuntimeException("Error fetching page: " + errorPage.errorMessage, errorPage.exception);
            } else if (page instanceof PageFetcher.PoisonPill) {
                pageFetcher.shutdownAwait();
                pageFetcher = null;
                page = null;
                rowCount++;
            }
        }
    }

    @Override
    public Traverser next() {
        if (!hasNext())
            throw new NoSuchElementException("No more elements.");

        if (!page.keyRecords.hasNext()) {
            throw new NoSuchElementException("No more elements. Please contact support.");
        }
        final KeyRecord kr = page.keyRecords.next();
        final FireflyVertex vertex = graph.vertexFromRecord(kr);
        Traverser t = tg.generate(vertex, graphStep, 1L);
        t.asAdmin().setStepId(startStep);
        return t;
    }

    @Override
    public void close() {
        if (page != null) {
            final PaginationIterator pi = (PaginationIterator) page.keyRecords;
            pi.close();
            page = null;
        }
        if (pageFetcher != null) {
            pageFetcher.shutdownAwait();
            pageFetcher = null;
        }
        if (pageQueue != null) {
            pageQueue.clear();
            pageQueue = null;
        }
    }
}
