package com.aerospike.firefly.olap;

import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryManagementTests {
    private Configuration config;

    @Before
    public void beforeEach() {
        config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final DistributedAerospikeConnection db = new DistributedAerospikeConnection(graph.getBaseGraph(), 0, 0);
            db.removeAllJobs();
        }
    }

    @Test
    public void jobListTest() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            // do some jobs
            graph.traversal().withComputer().V().toList();
            graph.traversal().withComputer().V().limit(2).toList();
            try {
                graph.traversal().withComputer().V().both().both().fail().toList();
            } catch (final Exception ignored) {
            }

            final List jobs = graph.traversal()
                    .withComputer()
                    .call("aerospike.graph.analytics.job.list")
                    .has("state", "FINISHED")
                    .elementMap()
                    .toList();

            assertEquals(2, jobs.size());

            graph.traversal()
                    .withComputer()
                    .call("aerospike.graph.analytics.job.clearLog")
                    .iterate();

            final List jobsAfterClear = graph.traversal()
                    .withComputer()
                    .call("aerospike.graph.analytics.job.list")
                    .toList();

            assertEquals(0, jobsAfterClear.size());
        }
    }

    @Ignore
    @Test
    public void cancelAllJobsTest() throws InterruptedException, IOException {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            final AtomicBoolean failed = new AtomicBoolean(false);
            final AtomicBoolean errorMessageFound = new AtomicBoolean(false);

            try (final OutputCapturer outputCapturer = new OutputCapturer()) {
                final Thread olapThread = new Thread(() -> {
                    System.out.println("Thread started");
                    try {
                        graph.traversal()
                                // .with("aerospike.graph.olap.debug.df", "true")
                                .withComputer()
                                .V().both().both().both().both().both().both().both().both()
                                .count()
                                .toList();
                    } catch (final Exception ignored) {
                        failed.set(true);
                    }
                });
                olapThread.start();

                // give some time to start. 5-10 seconds works locally
                Thread.sleep(10000);

                // try to kill all tasks
                graph.traversal()
                        .withComputer()
                        .call("aerospike.graph.analytics.job.cancel")
                        .iterate();

                olapThread.join();

                // verify pagerank task killed
                final String[] logList = outputCapturer.getLines();
                for (final String line : logList) {
                    if (line.contains("WARN  c.a.f.o.s.JobCancellationService - All jobs cancelled.")) {
                        errorMessageFound.set(true);
                        break;
                    }
                }
            }

            final List jobs = graph.traversal()
                    .withComputer()
                    .call("aerospike.graph.analytics.job.list")
                    .has("state", "CANCELLED")
                    .elementMap()
                    .toList();

            assertEquals(1, jobs.size());

            assertTrue(failed.get());
            assertTrue(errorMessageFound.get());
        }
    }
}