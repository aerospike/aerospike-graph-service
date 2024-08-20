package com.aerospike.firefly.process;

import com.aerospike.firefly.io.aerospike.admin.AuthenticationException;
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

    final String validRead = JWT.create()
            .withClaim("role", "READ")
            .withSubject("lyndon_loser")
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
    public void testEnabledAdmin() {
        PrintStream originalOut = System.out;
        final List<String> linesReport = new ArrayList<>();
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baos));
            createServerWithAuth(true);
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

    @Test
    public void testEnabledRead() {
        PrintStream originalOut = System.out;
        final List<String> linesReport = new ArrayList<>();
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baos));
            createServerWithAuth(true);
            Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).
                    credentials("lyndon_loser", validRead).create();
            final DriverRemoteConnection drc = DriverRemoteConnection.using(cluster);
            final GraphTraversalSource g = traversal().withRemote(drc);
            try {
                g.V().drop().iterate();
                Assert.fail("Should not be able to drop vertices.");
            } catch (AuthenticationException ignored) {
            }
            try {
                Vertex v = g.addV("foo").next();
                Assert.fail("Should not be able to drop vertices.");
            } catch (AuthenticationException ignored) {
            }
            try {
                g.mergeV(asMap(T.id, "foo1")).next();
                Assert.fail("Should not be able to drop vertices.");
            } catch (AuthenticationException ignored) {
            }
            final HashMap<Object, Object> mergeMap = new HashMap<>();
            mergeMap.put(Direction.OUT, new ReferenceVertex("1"));
            mergeMap.put(Direction.IN, new ReferenceVertex("1"));
            mergeMap.put(T.label, "mergeE");
            try {
                g.mergeE(mergeMap).next();
                Assert.fail("Should not be able to drop vertices.");
            } catch (AuthenticationException ignored) {
            }
            try {
                Edge e = g.addE("bar").from(__.V()).to(__.V()).next();
                Assert.fail("Should not be able to drop vertices.");
            } catch (AuthenticationException ignored) {
            }
            try {
                g.addE("bar").from(__.V()).to(__.V()).next();
                Assert.fail("Should not be able to drop vertices.");
            } catch (AuthenticationException ignored) {
            }
            try {
                g.E("1").drop().iterate();
                Assert.fail("Should not be able to drop vertices.");
            } catch (AuthenticationException ignored) {
            }
            try {
                g.V("1").drop().iterate();
                Assert.fail("Should not be able to drop vertices.");
            } catch (AuthenticationException ignored) {
            }
            try {
                g.call("aerospike.graph.admin.index.create").next();
                Assert.fail("Should not be able to drop vertices.");
            } catch (AuthenticationException ignored) {
            }
            boolean foundAddV = false;
            boolean foundAddE = false;
            boolean foundCreateSindex = false;
            boolean foundDrop = false;
            boolean foundDroppedV = false;
            boolean foundDroppedE = false;
            boolean foundMergeV = false;
            boolean foundMergeE = false;

            boolean foundAddVFailed = false;
            boolean foundAddEFailed = false;
            boolean foundCreateSindexFailed = false;
            boolean foundDropFailed = false;
            boolean foundDroppedVFailed = false;
            boolean foundDroppedEFailed = false;
            boolean foundMergeVFailed = false;
            boolean foundMergeEFailed = false;

            final List<String> logList = new ArrayList<>(Arrays.asList(baos.toString().split("\n")));
            for (String line : logList) {
                if (line.contains("{lyndon_loser}")) {
                    if (line.contains("created vertex with id: foo1")) {
                        foundMergeV = true;
                    } else if (line.contains("created vertex with id:")) {
                        foundAddV = true;
                    } else if (line.contains("created edge: ") && line.contains("mergeE")) {
                        foundMergeE = true;
                    } else if (line.contains("created edge: ")) {
                        foundAddE = true;
                    } else if (line.contains("summary - Get graph summary.")) {
                        foundCreateSindex = true;
                    } else if (line.contains("Dropped entire database.")) {
                        foundDrop = true;
                    } else if (line.contains("Dropped vertex with id: ")) {
                        foundDroppedV = true;
                    } else if (line.contains("Dropped edge ")) {
                        foundDroppedE = true;
                    } else if (line.contains("Insufficient permissions")) {
                        if (line.contains("Query: 'g.V().drop()")) {
                            foundDropFailed = true;
                        } else if (line.contains("Query: 'g.addV(\"foo\")")) {
                            foundAddVFailed = true;
                        } else if (line.contains("Query: 'g.mergeV")) {
                            foundMergeVFailed = true;
                        } else if (line.contains("Query: 'g.mergeE")) {
                            foundMergeEFailed = true;
                        } else if (line.contains("Query: 'g.addE(\"bar\")")) {
                            foundAddEFailed = true;
                        } else if (line.contains("Query: 'g.E(\"1\").drop()")) {
                            foundDroppedEFailed = true;
                        } else if (line.contains("Query: 'g.V(\"1\").drop()")) {
                            foundDroppedVFailed = true;
                        } else if (line.contains("aerospike.graph.admin.index.create")) {
                            foundCreateSindexFailed = true;
                        }
                    }
                    linesReport.add(line);
                }
            }
            Assert.assertFalse(foundAddE);
            Assert.assertFalse(foundAddV);
            Assert.assertFalse(foundCreateSindex);
            Assert.assertFalse(foundDrop);
            Assert.assertFalse(foundDroppedV);
            Assert.assertFalse(foundDroppedE);
            Assert.assertFalse(foundMergeV);
            Assert.assertFalse(foundMergeE);
            Assert.assertTrue(foundAddVFailed);
            Assert.assertTrue(foundAddEFailed);
            Assert.assertTrue(foundCreateSindexFailed);
            Assert.assertTrue(foundDropFailed);
            Assert.assertTrue(foundDroppedVFailed);
            Assert.assertTrue(foundDroppedEFailed);
            Assert.assertTrue(foundMergeVFailed);
            Assert.assertTrue(foundMergeEFailed);
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
