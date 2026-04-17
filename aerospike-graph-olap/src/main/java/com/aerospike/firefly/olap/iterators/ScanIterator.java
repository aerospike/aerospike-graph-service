/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.olap.iterators;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.io.aerospike.query.paged.PageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.PaginationIterator;
import com.aerospike.firefly.io.aerospike.query.paged.ScanPageFetcher;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyEdgeFactory;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
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
    LinkedBlockingQueue<PageFetcher.Page> pageQueue;
    final List<Row> rows = new ArrayList<>();
    final List<HasContainer> hasContainers;
    final String startStep;
    final Codec codec;
    final Traversal traversal;
    final TraverserGenerator tg;
    final GraphStep graphStep;
    final Expression expression;
    int rowCount = 0;
    PageFetcher<?> pageFetcher = null;
    PageFetcher.Page page = null;
    Iterator<ByteBuffer> edgeIterator;
    FireflyEdgeRecord edgeRecord;

    public ScanIterator(final FireflyGraph graph,
                        final GraphStep graphStep,
                        final HasContainer hasContainer,
                        final List<HasContainer> hasContainers,
                        final String startStep,
                        final Codec codec,
                        final Iterator<Row> iterator,
                        final Traversal traversal,
                        final Expression expression,
                        final TraverserGenerator tg) {
        this.tg = tg;
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
        pageQueue = new LinkedBlockingQueue<>(graph.getBaseGraph().getConfig().paginationPageQueueSize);
    }

    @Override
    public boolean hasNext() {
        while (true) {
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
                                graphStep.returnsVertex() ? graph.getBaseGraph().getConfig().vertexAeroSet : graph.getBaseGraph().getConfig().edgeAeroSet,
                                null,
                                graph.getBaseGraph().getConfig().paginationPageSize,
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
        }
    }

    private void verifyPageHasNext() {
        if (page == null || !page.keyRecords.hasNext()) {
            if (page.keyRecords != null) {
                final PaginationIterator pi = (PaginationIterator) page.keyRecords;
                pi.close();
            }
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
                final Map<ByteBuffer, Object> edgeData = (Map<ByteBuffer, Object>) keyRecord.record.getMap(graph.getBaseGraph().getConfig().edgeDataBin);
                edgeIterator = edgeData.keySet().iterator();
                edgeRecord = new FireflyEdgeRecord(keyRecord.record, this.graph.getBaseGraph());
            }

            final ByteBuffer edgeIdBytes = edgeIterator.next();
            final FireflyPhatEdgeId edgeId = graph.getIdFactory().createEdgeId(edgeIdBytes);

            final FireflyEdge edge = FireflyEdgeFactory.create(edgeId, edgeRecord, graph);
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
