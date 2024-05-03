package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.io.aerospike.query.paged.PagedGraphQuery;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalGraphComputerTest extends AbstractFireflySuite {

    private static final Logger LOG = LoggerFactory.getLogger(LocalGraphComputerTest.class);

    @Override
    protected boolean clearData() {
        graph.vertices().forEachRemaining(Vertex::remove);
        assertEquals(0L, graph.getVertexCount(List.of()));
        assertEquals(0L, graph.getEdgeCount());
        return true;
    }

    @Test
    public void testVertexPage() {
        for (int i = 0; i < 2000; i++) {
            Vertex v = graph.addVertex();
            v.addEdge("self",v);
            if (i % 100 == 0)
                LOG.warn("Loaded {} vertices", i);
        }
        graph.traversal().withComputer().V().count().profile().forEachRemaining(System.out::println);



        //assertEquals(10_00L, graph.getVertexCount(List.of()));
        //assertEquals(10_00L, graph.traversal().withComputer().withStrategies(VertexProgramStrategy.build().workers(16).create()).V().count().next().longValue());
    }

    @Test
    public void testStringId() {
        GraphTraversalSource h = graph.traversal();
        GraphTraversalSource g = graph.traversal().withComputer();

        GraphTraversal.Admin<?, ?> a = h.V().has("name", "marko").out().count().asAdmin();
        a.applyStrategies();
        System.out.println(a.getStartStep());

        graph.addVertex();
        graph.addVertex();
        System.out.println(g.V().out().count().explain().toString());
        assertEquals(2L, g.V().count().next().longValue());
        PagedGraphQuery pq = new PagedGraphQuery(graph);
        IteratorUtils.forEach(pq.scanVertexIds(), id -> {
            System.out.println(id);
        });
    }
}
