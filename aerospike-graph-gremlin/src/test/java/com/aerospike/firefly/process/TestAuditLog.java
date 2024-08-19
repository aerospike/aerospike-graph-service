package com.aerospike.firefly.process;

import com.aerospike.firefly.runtime.FireflyServer;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;

public class TestAuditLog {

    final String validAdmin = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));

    private static FireflyServer server;
    private static final String auditLogWithJWT = "../conf/credentials-config/firefly-gremlin-server-audit-log-jwt.yaml";
    private static final String noAuditLogJWT = "../conf/credentials-config/firefly-gremlin-server-no-audit-log-jwt.yaml";

    private void createServerWithAuth(final boolean auditLogEnabled) throws Exception {
        if (server != null) {
            server.stop().join();
        }
        if (auditLogEnabled) {
            server = FireflyServer.main(new String[]{auditLogWithJWT});
        } else {
            server = FireflyServer.main(new String[]{noAuditLogJWT});
        }
    }

    @Test
    public void testNotEnabled() throws Exception {
        PrintStream originalOut = System.out;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baos));
            System.clearProperty("FIREFLY_TESTING");
            createServerWithAuth(false);
            Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).
                    credentials("lyndon_username", validAdmin).create();
            final DriverRemoteConnection drc = DriverRemoteConnection.using(cluster);
            final GraphTraversalSource g = traversal().withRemote(drc);
            g.V().drop().iterate();
            Vertex v = g.addV("foo").next();
            g.mergeV(asMap(T.id, "foo1")).next();
            final HashMap<Object, Object> mergeMap = new HashMap<>();
            mergeMap.put(Direction.OUT, new ReferenceVertex(v.id()));
            mergeMap.put(Direction.IN, new ReferenceVertex(v.id()));
            mergeMap.put(T.label, "mergeE");
            g.mergeE(mergeMap).next();
            Edge e = g.addE("bar").from(__.V()).to(__.V()).next();
            g.addE("bar").from(__.V()).to(__.V()).next();
            g.E(e.id()).drop().iterate();
            g.V(v.id()).drop().iterate();
            g.call("summary").next();
            boolean foundAddV = false;
            boolean foundAddE = false;
            boolean foundSummary = false;
            boolean foundDrop = false;
            boolean foundDroppedV = false;
            boolean foundDroppedE = false;
            boolean foundMergeV = false;
            boolean foundMergeE = false;
            final List<String> logList = new ArrayList<>(Arrays.asList(baos.toString().split("\n")));
            for (String line : logList) {
                if (line.contains("{lyndon_username}")) {
                    if (line.contains("created vertex with id: foo1")) {
                        foundMergeV = true;
                    } else if (line.contains("created vertex with id:")) {
                        foundAddV = true;
                    } else if (line.contains("created edge: ") && line.contains("mergeE")) {
                        foundMergeE = true;
                    } else if (line.contains("created edge: ")) {
                        foundAddE = true;
                    } else if (line.contains("summary - Get graph summary.")) {
                        foundSummary = true;
                    } else if (line.contains("Dropped entire database.")) {
                        foundDrop = true;
                    } else if (line.contains("Dropped vertex with id: ")) {
                        foundDroppedV = true;
                    } else if (line.contains("Dropped edge ")) {
                        foundDroppedE = true;
                    }
                }
            }
            Assert.assertTrue(foundAddE);
            Assert.assertTrue(foundAddV);
            Assert.assertTrue(foundSummary);
            Assert.assertTrue(foundDrop);
            Assert.assertTrue(foundDroppedV);
            Assert.assertTrue(foundDroppedE);
            Assert.assertTrue(foundMergeV);
            Assert.assertTrue(foundMergeE);
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    public void testEnabled() {
        PrintStream originalOut = System.out;
        final List<String> linesReport = new ArrayList<>();
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baos));
            createServerWithAuth(true);
            System.clearProperty("FIREFLY_TESTING");
            Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).
                    credentials("lyndon_username", validAdmin).create();
            final DriverRemoteConnection drc = DriverRemoteConnection.using(cluster);
            final GraphTraversalSource g = traversal().withRemote(drc);
            g.V().drop().iterate();
            Vertex v = g.addV("foo").next();
            g.mergeV(asMap(T.id, "foo1")).next();
            final HashMap<Object, Object> mergeMap = new HashMap<>();
            mergeMap.put(Direction.OUT, new ReferenceVertex(v.id()));
            mergeMap.put(Direction.IN, new ReferenceVertex(v.id()));
            mergeMap.put(T.label, "mergeE");
            g.mergeE(mergeMap).next();
            Edge e = g.addE("bar").from(__.V()).to(__.V()).next();
            g.addE("bar").from(__.V()).to(__.V()).next();
            g.E(e.id()).drop().iterate();
            g.V(v.id()).drop().iterate();
            g.call("summary").next();
            boolean foundAddV = false;
            boolean foundAddE = false;
            boolean foundSummary = false;
            boolean foundDrop = false;
            boolean foundDroppedV = false;
            boolean foundDroppedE = false;
            boolean foundMergeV = false;
            boolean foundMergeE = false;
            final List<String> logList = new ArrayList<>(Arrays.asList(baos.toString().split("\n")));
            for (String line : logList) {
                if (line.contains("{lyndon_username}")) {
                    if (line.contains("created vertex with id: foo1")) {
                        foundMergeV = true;
                    } else if (line.contains("created vertex with id:")) {
                        foundAddV = true;
                    } else if (line.contains("created edge: ") && line.contains("mergeE")) {
                        foundMergeE = true;
                    } else if (line.contains("created edge: ")) {
                        foundAddE = true;
                    } else if (line.contains("summary - Get graph summary.")) {
                        foundSummary = true;
                    } else if (line.contains("Dropped entire database.")) {
                        foundDrop = true;
                    } else if (line.contains("Dropped vertex with id: ")) {
                        foundDroppedV = true;
                    } else if (line.contains("Dropped edge ")) {
                        foundDroppedE = true;
                    }
                    linesReport.add(line);
                }
            }
            Assert.assertTrue(foundAddE);
            Assert.assertTrue(foundAddV);
            Assert.assertTrue(foundSummary);
            Assert.assertTrue(foundDrop);
            Assert.assertTrue(foundDroppedV);
            Assert.assertTrue(foundDroppedE);
            Assert.assertTrue(foundMergeV);
            Assert.assertTrue(foundMergeE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            System.setOut(originalOut);
            for (final String lineReport : linesReport) {
                System.out.println(lineReport);
            }
        }
    }

    @AfterClass
    public static void tearDown() {
        if (server != null) {
            server.stop().join();
        }
    }
}
