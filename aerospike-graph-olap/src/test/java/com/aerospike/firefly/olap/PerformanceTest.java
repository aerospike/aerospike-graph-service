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

    // Ad-hoc performance harness. All tests here are @Ignore'd and require a
    // running Gremlin Server reachable at the host/port below; override via the
    // GREMLIN_HOST / GREMLIN_PORT environment variables when running locally.
    private static final String GREMLIN_HOST =
            System.getenv().getOrDefault("GREMLIN_HOST", "localhost");
    private static final int GREMLIN_PORT = Integer.parseInt(
            System.getenv().getOrDefault("GREMLIN_PORT", "8182"));

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
        try (GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(GREMLIN_HOST, GREMLIN_PORT))) {
            for (int i = 1; i < 6; i++) {
                final long start = Instant.now().toEpochMilli();

                // with("aerospike.graph.analytics.debug.df", "true")
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
        try (GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(GREMLIN_HOST, GREMLIN_PORT))) {
            for (int i = 2; i < 6; i++) {
                final long start = Instant.now().toEpochMilli();

                // with("aerospike.graph.analytics.debug.df", "true")
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
        try (GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(GREMLIN_HOST, GREMLIN_PORT))) {
            final long start = Instant.now().toEpochMilli();

            // with("aerospike.graph.analytics.debug.df", "true")
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
