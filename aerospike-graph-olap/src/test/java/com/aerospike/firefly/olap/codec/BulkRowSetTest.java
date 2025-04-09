package com.aerospike.firefly.olap.codec;

import com.aerospike.firefly.olap.Tokens;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

public class BulkRowSetTest {
    private Configuration config;

    @Before
    public void beforeEach() {
        config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
    }

    @Test
    public void testBulkRowSetCollision() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Load modern graph.
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final List<Vertex> vertices = graph.traversal().V().toList();

            // Traversal that has bulking support.
            final Traversal t = graph.traversal().V().
                    branch(__.label().is("person").count()).
                    option(1L, __.values("age")).
                    option(0L, __.values("lang")).
                    option(0L, __.values("name"));


            // Create a BulkedRowSet instance

            final Codec codec = new Codec(t);
            final BulkedRowSet bulkedRowSet = new BulkedRowSet(codec);

            // Add some rows to the set
            bulkedRowSet.add(new Colliding_Traverser<>(vertices.get(0), 1L));
            bulkedRowSet.add(new Colliding_Traverser<>(vertices.get(1), 1L));
            bulkedRowSet.add(new Colliding_Traverser<>(vertices.get(0), 3L));

            final Queue<Row> collidedRows = bulkedRowSet.getCollidedRows();
            Assert.assertEquals(1, collidedRows.size());
            Assert.assertEquals(1L, collidedRows.peek().get(codec.getBulkedOrdinal()));

            final Iterator<Row> rows = bulkedRowSet.iterator();
            Assert.assertTrue(rows.hasNext());
            final Row firstRow = rows.next();
            Assert.assertEquals(4L, firstRow.get(codec.getBulkedOrdinal()));

            Assert.assertTrue(rows.hasNext());
            final Row secondRow = rows.next();
            Assert.assertEquals(1L, secondRow.get(codec.getBulkedOrdinal()));
            Assert.assertFalse(rows.hasNext());
        }
    }

    class Colliding_Traverser<T> extends B_O_Traverser<T> {
        public Colliding_Traverser(final T t, final long bulkCount) {
            super(t, bulkCount);
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String getStepId() {
            return "";
        }

        @Override
        public boolean equals(final Object obj) {
            return false;
        }
    }
}
