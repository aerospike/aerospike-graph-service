package com.aerospike.firefly.olap;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class PlayTest {

    private final static List<Function<GraphTraversalSource, GraphTraversal>> queries = new ArrayList<>() {{
        add(null); // for counting %)
        add(g -> g.V().hasLabel(":InternalId").count()); //1
        add(g -> g.V().hasLabel(":InternalLabel").where(__.inE().count().is(P.gte(3))).count()); //2
        add(g -> g.V().hasLabel(":InternalLabel").groupCount().by(__.in().count())); //3
        add(g -> g.V().groupCount().by(__.out().count())); //4
        add(g -> g.V().hasLabel(":InternalLabel").in("HAS_RELATION").count()); //5
        add(g -> g.E().hasLabel("STRICT").count()); //6
        add(g -> g.V().hasLabel(":InternalLabel").has("value", TextP.startingWith("380")).
                where(__.in().count().is(P.gte(5)))); //7
        add(g -> g.V().hasLabel(":InternalId").out().groupCount().by(__.out().count())); //8
    }};

    // @Test
    public void performanceTest() {
        // 35.193.63.202 - lyndon's
        // 34.135.193.8 - ishaan's
        try (GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("34.135.193.8", 8182))) {
            for (int i = 7; i < 8; i++) {

                final Instant instant = Instant.now();

                // with("aerospike.graph.olap.debug.df", "true")
                final GraphTraversalSource seed = g.with("evaluationTimeout", 900 * 1000).
                        with("aerospike.client.batch.read.size", 5000).
                        with("aerospike.graph.pagination.page.size", 5000).
                        withComputer();

                final List result = queries.get(i).apply(seed).toList();
                System.out.println("Result " + i + ": " + result);
                System.out.println("Total time: " + (Instant.now().toEpochMilli() - instant.toEpochMilli()) + " ms.\n");
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            System.out.println("Failed " + e);
        }
    }
}
