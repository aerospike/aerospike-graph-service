package com.aerospike.firefly.olap.iterators;

import com.aerospike.firefly.olap.codec.RowCodec;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.aerospike.firefly.olap.codec.RowCodecHelper.getId;
import static com.aerospike.firefly.process.traversal.step.util.TraversalUtil.fireflyTestAll;

public class PIIterator {
    public static Iterator<Traverser> getIterator(final FireflyGraph graph,
                                    final GraphStep graphStep,
                                    final List<HasContainer> hasContainers,
                                    final Step startStep,
                                    final TraverserGenerator tg,
                                    final Iterator<Row> iterator) {

        final List<FireflyId> ffids = new ArrayList<>();
        while (iterator.hasNext()) {
            final Row row = iterator.next();
            final Object id = getId(
                    row.getString(row.fieldIndex(RowCodec.ID_COL)),
                    row.getInt(row.fieldIndex(RowCodec.ID_TYPEHINT_COL)));
            ffids.add(graphStep.returnsVertex() ? graph.getIdFactory().createVertexId(id) : graph.getIdFactory().createEdgeId(id));
        }
        final List<List<FireflyId>> partitionedFfidList = Lists.partition(ffids, graph.getBaseGraph().getConfig().aerospikeBatchReadSize);
        final List<Traverser> traversers = new ArrayList<>();
        if (graphStep.returnsVertex()) {
            for (final List<FireflyId> ffidList : partitionedFfidList) {
                // TODO: Pushdown.
                List<FireflyVertex> vertices = graph.readVertices(List.of(), ffidList, null);
                for (final Vertex vertex : vertices) {
                    if (fireflyTestAll(vertex, hasContainers)) {
                        Traverser t = tg.generate(vertex, graphStep, 1l);
                        t.asAdmin().setStepId(startStep.getId());
                        traversers.add(t);
                    }
                }
            }
        } else {
            for (final List<FireflyId> ffidList : partitionedFfidList) {
                List<FireflyEdge> edges = graph.readEdges(List.of(), ffidList, null);
                for (final FireflyEdge edge : edges) {
                    if (fireflyTestAll(edge, hasContainers)) {
                        Traverser t = tg.generate(edge, startStep, 1l);
                        t.asAdmin().setStepId(startStep.getId());
                        traversers.add(t);
                    }
                }
            }
        }
        return traversers.iterator();
    }
}
