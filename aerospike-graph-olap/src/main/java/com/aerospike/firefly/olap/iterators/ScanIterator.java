package com.aerospike.firefly.olap.iterators;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.io.aerospike.query.paged.PageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.PaginationIterator;
import com.aerospike.firefly.io.aerospike.query.paged.PartitionedSindexPageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.ScanPageFetcher;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.structure.DistributedWorkerExecutor;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyEdgeFactory;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.TimeoutHelper;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static com.aerospike.firefly.olap.structure.DistributedWorkerExecutor.COUNT_COL;
import static com.aerospike.firefly.olap.structure.DistributedWorkerExecutor.START_COL;

public class ScanIterator implements CloseableIterator<Traverser> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanIterator.class);
    final FireflyGraph graph;
    final LinkedBlockingQueue<PageFetcher.Page> pageQueue;
    final List<Row> rows = new ArrayList<>();
    int rowCount = 0;
    final List<HasContainer> hasContainers;
    final String startStep;
    final Codec codec;
    final Traversal traversal;
    final TraverserGenerator tg;
    final TraversalMatrix tm;
    PageFetcher<?> pageFetcher = null;
    PageFetcher.Page page = null;
    final GraphStep graphStep;
    final Expression expression;
    Iterator<Map.Entry<ByteBuffer, List>> edgeIterator;
    KeyRecord kr = null;

    public ScanIterator(final FireflyGraph graph,
                        final GraphStep graphStep,
                        final HasContainer hasContainer,
                        final List<HasContainer> hasContainers,
                        final String startStep,
                        final Codec codec,
                        final Iterator<Row> iterator,
                        final Traversal traversal,
                        final TraversalMatrix tm,
                        final Expression expression,
                        final TraverserGenerator tg) {
        this.tg = tg;
        this.tm = tm;
        this.graph = graph;
        while (iterator.hasNext()) {
            rows.add(iterator.next());
        }
        this.hasContainers = hasContainers;
        this.startStep = startStep;
        this.codec = codec;
        this.traversal = traversal;
        this.graphStep = graphStep;
        this.expression = expression;
        pageQueue = new LinkedBlockingQueue<>(graph.getBaseGraph().PAGINATION_PAGE_QUEUE_SIZE);
    }

    @Override
    public boolean hasNext() {
        if (edgeIterator != null && edgeIterator.hasNext()) {
            return true;
        }

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

                    TaskLogger.logDebuggingMessage("Row number " + rowCount, LOGGER);

                    // TODO: Can omit some bins here.
                    final int evaluationTimeout = Long.valueOf(TimeoutHelper.calculate(traversal.asAdmin())).intValue();

                    final ScanPolicy policy = new ScanPolicy();
                    policy.setTimeout(evaluationTimeout);
                    policy.filterExp = expression;
                    attemptCount++;
                    pageFetcher = new ScanPageFetcher<>(
                            graph,
                            policy,
                            graphStep.returnsVertex() ? graph.getBaseGraph().VERTEX_AERO_SET : graph.getBaseGraph().EDGE_AERO_SET,
                            null,
                            graph.getBaseGraph().PAGINATION_PAGE_SIZE,
                            null,
                            partitionFilter,
                            Executors.newSingleThreadExecutor(),
                            pageQueue,
                            graph::vertexFromRecord);

                    // Intentionally not using return value here.
                    pageFetcher.startQueryDirect();
                    break;
                } catch (final Exception e) {
                    TaskLogger.logDebuggingMessage("Got exception " + e.getMessage(), LOGGER);
                    if (attemptCount > 10) {
                        throw new RuntimeException("Failed to run query after " + attemptCount + " attempts.", e);
                    } else if (e.getMessage().contains("Operation not allowed at this time")) {
                        TaskLogger.logDebuggingMessage("Sleeping.", LOGGER);
                        try {
                            Thread.sleep(1000L * (attemptCount + 1));
                        } catch (InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    }
                }
            }
        }

        try {
            page = pageQueue.take();
        } catch (InterruptedException e) {
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

        return hasNext();
    }

    private void verifyPageHasNext() {
        if (page == null || !page.keyRecords.hasNext()) {
            throw new NoSuchElementException("No more elements. Please contact support.");
        }
    }

    @Override
    public Traverser next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements.");
        }

        // TODO: Improve performance with ReferenceVertex.
        if (graphStep.returnsVertex()) {
            verifyPageHasNext();
            final KeyRecord keyRecord = page.keyRecords.next();
            final FireflyVertex vertex = graph.vertexFromRecord(keyRecord);
            Traverser t = tg.generate(vertex, graphStep, 1L);
            t.asAdmin().setStepId(startStep);
            return t;
        } else {
            if (edgeIterator == null || !edgeIterator.hasNext()) {
                verifyPageHasNext();
                final KeyRecord keyRecord = page.keyRecords.next();
                final Map<ByteBuffer, List> edgeData = (Map<ByteBuffer, List>) keyRecord.record.getMap(graph.getBaseGraph().EDGE_DATA_BIN);
                edgeIterator = edgeData.entrySet().iterator();
                kr = keyRecord;
            }

            final Map.Entry<ByteBuffer, List> entry = edgeIterator.next();
            final String label = (String) entry.getValue().get(FireflyEdge.LABEL_POSITION);

            final String outV = (String) entry.getValue().get(FireflyEdge.OUT_V_POSITION);
            final FireflyId outVertex = graph.getIdFactory().createVertexIdFromHash(outV);

            final String inV = (String) entry.getValue().get(FireflyEdge.IN_V_POSITION);
            final FireflyId inVertex = graph.getIdFactory().createVertexIdFromHash(inV);

            final Map<String, Object> properties = (Map<String, Object>) entry.getValue().get(FireflyEdge.PROPERTIES_POSITION);
            final Map<String, Object> typeHints = (Map<String, Object>) entry.getValue().get(FireflyEdge.TYPE_HINTS_POSITION);

            final Map<ByteBuffer, String> outSupernodes = (Map<ByteBuffer, String>) kr.record.getMap(graph.getBaseGraph().SUPERNODES_OUT_BIN);
            final Map<ByteBuffer, String> inSupernodes = (Map<ByteBuffer, String>) kr.record.getMap(graph.getBaseGraph().SUPERNODES_IN_BIN);
            final boolean isOutSupernode = outSupernodes != null && outSupernodes.containsKey(entry.getKey());
            final boolean isInSupernode = inSupernodes != null && inSupernodes.containsKey(entry.getKey());

            final FireflyEdge edge = FireflyEdgeFactory.create(graph.getIdFactory().createEdgeId(entry.getKey()), label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode, kr.record.generation);
            Traverser t = tg.generate(edge, graphStep, 1L);
            t.asAdmin().setStepId(startStep);
            return t;
        }
    }

    @Override
    public void close() {
        if (page != null) {
            final PaginationIterator pi = (PaginationIterator) page.keyRecords;
            pi.close();
        }
        if (pageFetcher != null) {
            pageFetcher.shutdownAwait();
        }
    }
}
