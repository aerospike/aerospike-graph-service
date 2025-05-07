package com.aerospike.firefly.olap;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class PerformanceTest {
    //a customer dataset queries
    //    private final static List<Function<GraphTraversalSource, GraphTraversal>> queries = new ArrayList<>() {{
    //        add(null); // for counting %)
    //        add(g -> g.V().hasLabel(":InternalId").count()); //1
    //        add(g -> g.V().hasLabel(":InternalLabel").where(__.inE().count().is(P.gte(3))).count()); //2
    //        add(g -> g.V().hasLabel(":InternalLabel").groupCount().by(__.in().count())); //3
    //        add(g -> g.V().groupCount().by(__.out().count())); //4
    //        add(g -> g.V().hasLabel(":InternalLabel").in("HAS_RELATION").count()); //5
    //        add(g -> g.E().hasLabel("STRICT").count()); //6
    //        add(g -> g.V().hasLabel(":InternalLabel").has("value", TextP.startingWith("380")).
    //                where(__.in().count().is(P.gte(5)))); //7
    //        add(g -> g.V().hasLabel(":InternalId").out().groupCount().by(__.out().count())); //8
    //    }};

    private final static List<Function<GraphTraversalSource, GraphTraversal>> queries = new ArrayList<>() {{
        add(null); // for counting %)
        add(g -> g.V(0).in()); //1
        add(g -> g.V().has("region", "REGION_1").where(__.out().hasId(0).count().is(1))); //2
        add(g -> g.V().has("region", "REGION_1")
                .where(__.out().hasId(0, 1, 2).count().is(3))); //3
        add(g -> g.V().has("region", "REGION_1")
                .and(__.out().hasId(0, 1, 2).count().is(3),
                        __.out().hasId(3, 4).count().is(0))); //4
        add(g -> g.V().has("region", "REGION_1")); // 5
    }};

    @Ignore
    @Test
    public void performanceTest() {
        try (GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("127.0.0.1", 8182))) {
            for (int i = 1; i < 6; i++) {
                final long start = Instant.now().toEpochMilli();

                // with("aerospike.graph.olap.debug.df", "true")
                final GraphTraversalSource seed = g.with("evaluationTimeout", 900 * 1000)
                        .with("aerospike.client.batch.read.size", 5000)
                        .with("aerospike.graph.pagination.page.size", 5000)
                        .withComputer();

                final List result = queries.get(i).apply(seed).count().toList();
                System.out.println("Result " + i + ": " + result);
                System.out.println("Total time: " + (Instant.now().toEpochMilli() - start) + " ms.\n");
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            System.out.println("Failed " + e);
        }
    }

    @Ignore
    @Test
    public void top100Test() {
        try (GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("127.0.0.1", 8182))) {
            for (int i = 2; i < 6; i++) {
                final long start = Instant.now().toEpochMilli();

                // with("aerospike.graph.olap.debug.df", "true")
                final GraphTraversalSource seed = g.with("evaluationTimeout", 600 * 1000)
                        .with("aerospike.client.batch.read.size", 5000)
                        .with("aerospike.graph.pagination.page.size", 5000)
                        .withComputer();

                final List result = queries.get(i).apply(seed).limit(100_000).toList();
                System.out.println("Result " + i + ": " + result.size());
                System.out.println("Total time: " + (Instant.now().toEpochMilli() - start) + " ms.\n");
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            System.out.println("Failed " + e);
        }
    }

    @Ignore
    @Test
    public void algorithmTest() {
        try (GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("127.0.0.1", 8182))) {
            final long start = Instant.now().toEpochMilli();

            // with("aerospike.graph.olap.debug.df", "true")
            final List result = g.with("evaluationTimeout", 900 * 1000)
                    .with("aerospike.client.batch.read.size", 5000)
                    .with("aerospike.graph.pagination.page.size", 5000)
                    .withComputer()
                    .V(1, 2, 3, 4, 5).pageRank().with(PageRank.times, 5).elementMap().toList();

            System.out.println("Result: " + result);
            System.out.println("Total time: " + (Instant.now().toEpochMilli() - start) + " ms.\n");
        } catch (Exception e) {
            System.out.println("Failed " + e);
        }
    }
}
