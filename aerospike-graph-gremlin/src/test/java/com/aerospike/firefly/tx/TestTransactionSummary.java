package com.aerospike.firefly.tx;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestTransactionSummary {
    private static final Logger LOG = LoggerFactory.getLogger(TestTransactionSummary.class);
    private static String VERTEX_COUNT = "Total vertex count";
    private static String VERTEX_COUNT_BY_LABEL = "Vertex count by label";
    private static String VERTEX_PROPERTIES_BY_LABEL = "Vertex properties by label";
    private static String EDGE_COUNT = "Total edge count";
    private static String EDGE_COUNT_BY_LABEL = "Edge count by label";
    private static String EDGE_PROPERTIES_BY_LABEL = "Edge properties by label";

    @Test
    public void testSummaryWithTxns() throws Exception {
        final DriverRemoteConnection connection = DriverRemoteConnection.using("localhost", 8182);
        final GraphTraversalSource g = traversal().withRemote(connection);
        try {
            g.V().drop().iterate();
            Assert.assertEquals(0, (long) g.V().count().next());

            // Test that uncommitted info is not added.
            GraphTraversalSource gtx1 = g.tx().begin();
            Object gid1 = gtx1.addV("label1").property("p1", "foo").next();
            Object gid2 = gtx1.addV("label2").next();
            gtx1.V(gid2).property("p2", "foo").next();
            gtx1.addE("label1").property("p1", "foo").property("p2", "foo").from(__.V(gid1)).to(__.V(gid2)).next();
            Thread.sleep(3333);
            Map<String, String> summaryLog = parseSummary(g);
            Assert.assertTrue(summaryLog.get(VERTEX_COUNT).contains(" 0."));
            Assert.assertTrue(summaryLog.get(VERTEX_COUNT_BY_LABEL).contains(" {}."));
            Assert.assertTrue(summaryLog.get(VERTEX_PROPERTIES_BY_LABEL).contains(" {}."));
            Assert.assertTrue(summaryLog.get(EDGE_COUNT).contains(" 0."));
            Assert.assertTrue(summaryLog.get(EDGE_COUNT_BY_LABEL).contains(" {}."));
            Assert.assertTrue(summaryLog.get(EDGE_PROPERTIES_BY_LABEL).contains(" {}."));
            gtx1.tx().rollback();

            // Test concurrent adding from a g and 2 gtx.
            gtx1 = g.tx().begin();
            GraphTraversalSource gtx2 = g.tx().begin();

            gid1 = g.addV("labelg").property("p1", "foo").property("pshared", "foo").next();
            Object gtx1id1 = gtx1.addV("labelgtx1").property("gtx1p1", "foo").property("pshared", "foo").next();
            Object gtx2id1 = gtx2.addV("labelgtx2").property("gtx2p1", "foo").property("pshared", "foo").next();

            gid2 = g.addV("labelshared").next();
            Object gtx1id2 = gtx1.addV("labelshared").next();
            Object gtx2id2 = gtx2.addV("labelshared").next();

            g.V(gid2).property("p2", "foo").property("pshared", "foo").next();
            gtx1.V(gtx1id2).property("gtx1p2", "foo").property("pshared", "foo").next();
            gtx2.V(gtx2id2).property("gtx2p2", "foo").property("pshared", "foo").next();

            g.addE("labelshared").property("p1", "foo").property("p2", "foo").from(__.V(gid1)).to(__.V(gid2)).next();
            gtx1.addE("labelshared").property("gtx1p1", "foo").property("gtx1p2", "foo").from(__.V(gtx1id1)).to(__.V(gtx1id2)).next();
            gtx2.addE("labelshared").property("gtx2p1", "foo").property("gtx2p2", "foo").from(__.V(gtx2id1)).to(__.V(gtx2id2)).next();

            Object eid = g.addE("labelg").from(__.V(gid1)).to(__.V(gid2)).id().next();
            Object gtx1eid = gtx1.addE("labelgtx1").from(__.V(gtx1id1)).to(__.V(gtx1id2)).id().next();
            Object gtx2eid = gtx2.addE("labelgtx2").from(__.V(gtx2id1)).to(__.V(gtx2id2)).id().next();

            g.E(eid).property("pshared", "foo").next();
            gtx1.E(gtx1eid).property("pshared", "foo").next();
            gtx2.E(gtx2eid).property("pshared", "foo").next();
            gtx1.tx().rollback();
            gtx2.tx().commit();

            Thread.sleep(3333);
            summaryLog = parseSummary(g);
            Assert.assertTrue(summaryLog.get(VERTEX_COUNT).contains(" 4."));
            Assert.assertTrue(summaryLog.get(VERTEX_COUNT_BY_LABEL).contains("labelg=1"));
            Assert.assertTrue(summaryLog.get(VERTEX_COUNT_BY_LABEL).contains("labelgtx2=1"));
            Assert.assertTrue(summaryLog.get(VERTEX_COUNT_BY_LABEL).contains("labelshared=2"));
            Assert.assertFalse(summaryLog.get(VERTEX_COUNT_BY_LABEL).contains("labelgtx1"));
            Assert.assertTrue(summaryLog.get(VERTEX_PROPERTIES_BY_LABEL).contains("labelg=[p1, pshared]"));
            Assert.assertTrue(summaryLog.get(VERTEX_PROPERTIES_BY_LABEL).contains("labelgtx2=[gtx2p1, pshared]"));
            Assert.assertTrue(summaryLog.get(VERTEX_PROPERTIES_BY_LABEL).contains("labelshared=[p2, gtx2p2, pshared]"));
            Assert.assertFalse(summaryLog.get(VERTEX_PROPERTIES_BY_LABEL).contains("labelgtx1"));
            Assert.assertFalse(summaryLog.get(VERTEX_PROPERTIES_BY_LABEL).contains("gtx1p1"));
            Assert.assertFalse(summaryLog.get(VERTEX_PROPERTIES_BY_LABEL).contains("gtx1p2"));
            Assert.assertTrue(summaryLog.get(EDGE_COUNT).contains(" 4."));
            Assert.assertTrue(summaryLog.get(EDGE_COUNT_BY_LABEL).contains("labelg=1"));
            Assert.assertTrue(summaryLog.get(EDGE_COUNT_BY_LABEL).contains("labelgtx2=1"));
            Assert.assertTrue(summaryLog.get(EDGE_COUNT_BY_LABEL).contains("labelshared=2"));
            Assert.assertFalse(summaryLog.get(EDGE_COUNT_BY_LABEL).contains("labelgtx1"));
            Assert.assertTrue(summaryLog.get(EDGE_PROPERTIES_BY_LABEL).contains("labelg=[pshared]"));
            Assert.assertTrue(summaryLog.get(EDGE_PROPERTIES_BY_LABEL).contains("labelgtx2=[pshared]"));
            Assert.assertTrue(summaryLog.get(EDGE_PROPERTIES_BY_LABEL).contains("labelshared=[p1, p2, gtx2p2, gtx2p1]"));
            Assert.assertFalse(summaryLog.get(EDGE_PROPERTIES_BY_LABEL).contains("labelgtx1"));
            Assert.assertFalse(summaryLog.get(EDGE_PROPERTIES_BY_LABEL).contains("gtx1p1"));
            Assert.assertFalse(summaryLog.get(EDGE_PROPERTIES_BY_LABEL).contains("gtx1p2"));
        } finally {
            g.V().drop().iterate();
            connection.close();
        }
    }

    private Map<String, String> parseSummary(final GraphTraversalSource g) {
        final String summary = (String) g.call("aerospike.graph.admin.metadata.summary").with("pretty").next();
        final List<String> lastTickerLines = summary.lines().collect(Collectors.toList());
        final Map<String, String> summaryInfo = new HashMap<>();
        for (final String line : lastTickerLines) {
            LOG.warn(line);
            if (line.contains(VERTEX_COUNT)) {
                summaryInfo.put(VERTEX_COUNT, line);
            } else if (line.contains(VERTEX_COUNT_BY_LABEL)) {
                summaryInfo.put(VERTEX_COUNT_BY_LABEL, line);
            } else if (line.contains(VERTEX_PROPERTIES_BY_LABEL)) {
                summaryInfo.put(VERTEX_PROPERTIES_BY_LABEL, line);
            } else if (line.contains(EDGE_COUNT)) {
                summaryInfo.put(EDGE_COUNT, line);
            } else if (line.contains(EDGE_COUNT_BY_LABEL)) {
                summaryInfo.put(EDGE_COUNT_BY_LABEL, line);
            } else if (line.contains(EDGE_PROPERTIES_BY_LABEL)) {
                summaryInfo.put(EDGE_PROPERTIES_BY_LABEL, line);
            }
        }
        Assert.assertEquals(6, summaryInfo.size());
        return summaryInfo;
    }
}
