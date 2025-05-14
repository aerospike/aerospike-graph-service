package com.aerospike.firefly.olap.iterators;

import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.paged.PageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.PaginationIterator;
import com.aerospike.firefly.io.aerospike.query.paged.PartitionedSindexPageFetcher;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.process.traversal.step.computer.FireflyBatchEdgeReadStepLocal;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyEdgeFactory;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static com.aerospike.firefly.olap.structure.DistributedWorkerExecutor.COUNT_COL;
import static com.aerospike.firefly.olap.structure.DistributedWorkerExecutor.FIRST_COL;
import static com.aerospike.firefly.olap.structure.DistributedWorkerExecutor.START_COL;

public class PIStepIterator implements CloseableIterator<Traverser> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexIterator.class);
    final FireflyGraph graph;
    final LinkedBlockingQueue<PageFetcher.Page> pageQueue;
    final List<Row> rows;
    int rowCount = 0;
    final String startStep;
    final Traversal traversal;
    final FireflyIndexMetadata.IndexInfo indexInfo;
    private final GraphStep graphStep;
    final Traverser start;
    PageFetcher<?> pageFetcher = null;
    PageFetcher.Page page = null;
    final FireflyId inputVertexId;
    final VertexStep vertexStep;
    final List<Edge> currentEdges;
    final Direction direction;
    final List<HasContainer> hasContainer;
    final Set<String> edgeLabels;

    public PIStepIterator(final FireflyGraph graph,
                          final FireflyId inputVertexId,
                          final GraphStep graphStep,
                          final VertexStep vertexStep,
                          final String startStep,
                          final FireflyIndexMetadata.IndexInfo indexInfo,
                          final List<Row> rows,
                          final Traversal traversal,
                          final Traverser start,
                          final Direction direction) {
        this.graphStep = graphStep;
        this.start = start;
        this.indexInfo = indexInfo;
        this.graph = graph;
        this.rows = rows;
        this.startStep = startStep;
        this.traversal = traversal;
        this.pageQueue = new LinkedBlockingQueue<>(graph.getBaseGraph().PAGINATION_PAGE_QUEUE_SIZE);
        this.inputVertexId = inputVertexId;
        this.vertexStep = vertexStep;
        this.currentEdges = new ArrayList<>();
        this.direction = direction;
        final FireflyBatchEdgeReadStepLocal edgeStep = (FireflyBatchEdgeReadStepLocal) vertexStep;
        this.hasContainer = new ArrayList<>(edgeStep.fireflyHasContainers);
        this.hasContainer.addAll(edgeStep.aerospikeHasContainers);
        this.edgeLabels = Set.of(edgeStep.getEdgeLabels());
        graph.logMessage("PIStepIterator created with " + rows.size() + " rows.", LOGGER);
    }

    @Override
    public boolean hasNext() {
        while(true) {
            if (!currentEdges.isEmpty()) {
                return true;
            }

            if (rows.isEmpty() || rows.size() == rowCount) {
                return false;
            }

            if (page != null) {
                while (currentEdges.isEmpty() && page.keyRecords.hasNext()) {
                    final KeyRecord kr = page.keyRecords.next();
                    getIndividualEdgeIdsAttachedToVertex(kr.record);
                }
                if (!currentEdges.isEmpty()) {
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
                        attemptCount++;
                        final Row row = rows.get(rowCount);
                        final PartitionFilter partitionFilter = PartitionFilter.range(
                                row.getInt(row.fieldIndex(START_COL)),
                                row.getInt(row.fieldIndex(COUNT_COL)));
                        final AerospikeConnection db = graph.getBaseGraph();

                        final String keyHashString = inputVertexId.getKeyHashString();
                        if (row.getBoolean(row.fieldIndex(FIRST_COL))) {
                            final FireflyVertex v = graph.readVertex(inputVertexId);
                            // Vertex doesn't exist, therefore no output from here.
                            if (v == null) {
                                return false;
                            }
                            final List<FireflyId> edgeIds = v.getCachedEdgeIds(direction, Set.of(vertexStep.getEdgeLabels()));
                            final List<FireflyEdge> cachedEdges = graph.readEdges(List.of(), edgeIds, null);
                            for (final FireflyEdge edge : cachedEdges) {
                                if (HasContainer.testAll(edge, hasContainer) && (edgeLabels.isEmpty() || edgeLabels.contains(edge.label()))) {
                                    currentEdges.add(edge);
                                }
                            }
                        }
                        if (direction == Direction.BOTH) {
                            throw new IllegalStateException("Error, cannot run this optimization on 'both'.");
                        }
                        final String indexName = direction == Direction.OUT ? db.E_OUT_INDEX_NAME : db.E_IN_INDEX_NAME;
                        final QueryPolicy queryPolicy = new QueryPolicy();
                        queryPolicy.sendKey = true;
                        queryPolicy.includeBinData = true;
                        // TODO: HasContainer support.
                        //final Set<String> labels = Set.of(vertexStep.getEdgeLabels());
                        //final String supernodeBin = direction == Direction.OUT ? db.SUPERNODES_OUT_BIN : db.SUPERNODES_IN_BIN;
                        //queryPolicy.filterExp = GraphQueryHelper.phatEdgeHasContainerListToExpression(db, hasContainers, labels,
                        //        keyHashString, null, direction);
                        pageFetcher = new PartitionedSindexPageFetcher<>(
                                graph,
                                queryPolicy,
                                db.EDGE_AERO_SET,
                                graph.getBaseGraph().getNamespace(),
                                Filter.contains(direction == Direction.OUT ? db.SUPERNODES_OUT_BIN : db.SUPERNODES_IN_BIN, IndexCollectionType.MAPVALUES, keyHashString),
                                graph.getBaseGraph().PAGINATION_PAGE_SIZE,
                                graph::vertexFromRecord,
                                indexName,
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

        if (currentEdges.isEmpty()) {
            throw new NoSuchElementException("No more elements. Please contact support.");
        }

        final Edge e = currentEdges.remove(0);
        final Traverser t = start.asAdmin().split(e, vertexStep);
        t.asAdmin().setStepId(startStep);
        return t;
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

    protected void getIndividualEdgeIdsAttachedToVertex(final Record record) {
        if (direction == Direction.BOTH) {
            // Direction.BOTH should not be propagated here and should be combined at a higher level.
            throw new RuntimeException("Cannot get individual Edge IDs attached to a Vertex with Direction.BOTH");
        }

        final FireflyEdgeRecord edgeRecord = new FireflyEdgeRecord(record, graph.getBaseGraph());
        final List<FireflyEdgeId> edgeIdsInRecord = edgeRecord.getEdgeIds();
        for (final FireflyEdgeId edgeId : edgeIdsInRecord) {
            final FireflyId vertexId;
            if (direction == Direction.OUT) {
                vertexId = edgeRecord.getOutV(edgeId);
            } else {
                vertexId = edgeRecord.getInV(edgeId);
            }
            if (this.inputVertexId.getKeyHashString().equals(vertexId.getKeyHashString())) {
                final Edge edge = FireflyEdgeFactory.create(edgeId, edgeRecord, graph);
                if (HasContainer.testAll(edge, hasContainer) && (edgeLabels.isEmpty() || edgeLabels.contains(edge.label()))) {
                    currentEdges.add(edge);
                }
            }
        }
    }
}
