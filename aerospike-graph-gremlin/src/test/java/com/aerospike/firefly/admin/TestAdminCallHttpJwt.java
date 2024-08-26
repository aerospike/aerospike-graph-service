package com.aerospike.firefly.admin;

import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.security.UserContext;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Base64;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestAdminCallHttpJwt {

    private static FireflyServer server;

    final String validRead = JWT.create()
            .withClaim("role", "READ")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));

    final String noRole = JWT.create()
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));

    final String validWrite = JWT.create()
            .withClaim("role", "READ_WRITE")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));

    final String validAdmin = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));

    interface Check {
        void check(final String userCredentials);
    }

    @Test
    public void testJwtGenerateCallStep() {
        final Cluster cluster = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("lyndon_username", validAdmin)
                .create();
        final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster);
        final GraphTraversalSource g = traversal().withRemote(connection);
        final String adminToken = (String) g.call("aerospike.graph.admin.rbac-jwt.issue-token").with("username", "lyndon_admin").with("role", "ADMIN").next();
        final Cluster cluster2 = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("lyndon_admin", adminToken)
                .create();
        final DriverRemoteConnection connection2 = DriverRemoteConnection.using(cluster2);
        final GraphTraversalSource g2 = traversal().withRemote(connection2);
        String readWriteToken = (String) g2.call("aerospike.graph.admin.rbac-jwt.issue-token").with("username", "lyndon_readwrite").with("role", "READ_WRITE").next();
        Cluster cluster3 = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("lyndon_readwrite", readWriteToken)
                .create();
        final DriverRemoteConnection connection3 = DriverRemoteConnection.using(cluster3);
        final GraphTraversalSource g3 = traversal().withRemote(connection3);
        Assert.assertThrows(Exception.class, () -> {
            g3.call("aerospike.graph.admin.rbac-jwt.issue-token").with("username", "lyndon_read").with("role", "READ").next();
        });
        g3.addV().iterate();
        g3.V().drop().iterate();

        final String readToken = (String) g.call("aerospike.graph.admin.rbac-jwt.issue-token").with("username", "lyndon_read").with("role", "READ").next();
        final Cluster cluster4 = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("lyndon_read", readToken)
                .create();
        final DriverRemoteConnection connection4 = DriverRemoteConnection.using(cluster4);
        final GraphTraversalSource g4 = traversal().withRemote(connection4);
        Assert.assertThrows(Exception.class, () -> {
            g4.addV().iterate();
        });
        Assert.assertThrows(Exception.class, () -> {
            g4.V().drop().iterate();
        });
        g4.V().count().next();
    }

    @Test
    public void testExpiry() throws InterruptedException {
        final Cluster cluster = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("lyndon_username", validAdmin)
                .create();
        final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster);
        final GraphTraversalSource g = traversal().withRemote(connection);
        final String adminToken = (String) g.call("aerospike.graph.admin.rbac-jwt.issue-token").with("username", "lyndon_admin").with("role", "ADMIN").with("expiry", 10).next();
        final Cluster cluster2 = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("lyndon_admin", adminToken)
                .create();
        final DriverRemoteConnection connection2 = DriverRemoteConnection.using(cluster2);
        final GraphTraversalSource g2 = traversal().withRemote(connection2);
        g2.V().count().next();
        Thread.sleep(11000);
        Assert.assertThrows(Exception.class, () -> {
            g2.V().count().next();
        });
        final Cluster cluster3 = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("lyndon_admin", adminToken)
                .create();
        final DriverRemoteConnection connection3 = DriverRemoteConnection.using(cluster3);
        final GraphTraversalSource g3 = traversal().withRemote(connection3);
        Assert.assertThrows(Exception.class, () -> {
            g3.V().count().next();
        });
    }

    public String adminIndexListHeaders(final String userCredentials) {
        try {
            final URL url = new URL("http://localhost:9090/admin/index/list");
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // Send request to the server and read reply
            final String token = "Bearer " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

            // Send request to the server and read reply
            con.setRequestMethod("GET");
            con.setRequestProperty("Authorization", token);

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }


    public String adminRbacJwtIssueToken(final String userCredentials) {
        try {
            final String query = String.format("username=%s&role=%s",
                    URLEncoder.encode("username", "UTF-8"),
                    URLEncoder.encode("ADMIN", "UTF-8"));

            final URL url = new URL("http://localhost:9090/admin/rbac-jwt/issue-token?" + query);
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // Send request to the server and read reply
            final String token = "Bearer " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

            // Send request to the server and read reply
            con.setRequestMethod("GET");
            con.setRequestProperty("Authorization", token);

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String adminIndexCreate(final String userCredentials) {
        try {
            final UUID uuid = UUID.randomUUID();
            final String query = String.format("property_key=%s&element_type=%s",
                    URLEncoder.encode(uuid.toString(), "UTF-8"),
                    URLEncoder.encode("vertex", "UTF-8"));
            final URL url = new URL("http://localhost:9090/admin/index/create?" + query);
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // Send request to the server and read reply
            final String token = "Bearer " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

            // Send request to the server and read reply
            con.setRequestMethod("GET");
            con.setRequestProperty("Authorization", token);

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            if (e.getMessage().startsWith("Index") && e.getMessage().contains("already exists"))
                return "good";
            throw new RuntimeException(e);
        }
    }

    public String adminIndexDrop(final String userCredentials) {
        try {
            final UUID uuid = UUID.randomUUID();
            final String query = String.format("property_key=%s&element_type=%s",
                    URLEncoder.encode(uuid.toString(), "UTF-8"),
                    URLEncoder.encode("vertex", "UTF-8"));
            final URL url = new URL("http://localhost:9090/admin/index/drop?" + query);
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // Send request to the server and read reply
            final String token = "Bearer " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

            // Send request to the server and read reply
            con.setRequestMethod("GET");
            con.setRequestProperty("Authorization", token);

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            if (e.getMessage().startsWith("Index") && e.getMessage().contains("already exists"))
                return "good";
            throw new RuntimeException(e);
        }
    }

    public String adminIndexStatus(final String userCredentials) {
        try {
            final String query = String.format("property_key=%s&element_type=%s",
                    URLEncoder.encode("foo", "UTF-8"),
                    URLEncoder.encode("vertex", "UTF-8"));
            final URL url = new URL("http://localhost:9090/admin/index/status?" + query);
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // Send request to the server and read reply
            final String token = "Bearer " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

            // Send request to the server and read reply
            con.setRequestMethod("GET");
            con.setRequestProperty("Authorization", token);

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            if (e.getMessage().startsWith("Index") && e.getMessage().contains("already exists"))
                return "good";
            throw new RuntimeException(e);
        }
    }

    public String adminIndexCardinality(final String userCredentials) {
        try {
            final URL url = new URL("http://localhost:9090/admin/index/cardinality");
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // Send request to the server and read reply
            final String token = "Bearer " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

            // Send request to the server and read reply
            con.setRequestMethod("GET");
            con.setRequestProperty("Authorization", token);

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String adminMetadataSummary(final String userCredentials) {
        try {
            final URL url = new URL("http://localhost:9090/admin/metadata/summary");
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // Send request to the server and read reply
            final String token = "Bearer " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

            // Send request to the server and read reply
            con.setRequestMethod("GET");
            con.setRequestProperty("Authorization", token);

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String adminMetadataUsage(final String userCredentials) {
        try {
            final URL url = new URL("http://localhost:9090/admin/metadata/usage");
            final HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // Send request to the server and read reply
            final String token = "Bearer " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

            // Send request to the server and read reply
            con.setRequestMethod("GET");
            con.setRequestProperty("Authorization", token);

            // Read input stream into String.
            final byte[] bytes = con.getInputStream().readAllBytes();
            final String response = new String(bytes);
            return response;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testNoRole() {
        try {
            adminIndexListHeaders(noRole);
            Assert.fail("Should not have been able to hit http endpoint no role");
        } catch (final Exception e) {
            Assert.assertEquals("java.io.IOException: Server returned HTTP response code: 401 for URL: http://localhost:9090/admin/index/list", e.getMessage());
        }
        final Cluster cluster = Cluster.build()
                .addContactPoint("localhost")
                .port(8182)
                .credentials("lyndon_username", noRole)
                .create();
        final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster);
        final GraphTraversalSource g = traversal().withRemote(connection);
        try {
            final String adminToken = (String) g.call("aerospike.graph.admin.rbac-jwt.issue-token").with("username", "lyndon_admin").with("role", "ADMIN").with("expiry", 10).next();
            Assert.fail("Should not have been able to issue token with no role");
        } catch (final Exception e) {
            Assert.assertEquals("org.apache.tinkerpop.gremlin.driver.exception.ResponseException: Failed to authorize: Unknown error in gremlin-server", e.getMessage());
        }
    }

    private void checkPermissions(final UserContext.ROLE requiredRole, final Check check) {
        if (requiredRole == UserContext.ROLE.READ) {
            check.check(validRead);
            check.check(validWrite);
            check.check(validAdmin);
        } else if (requiredRole == UserContext.ROLE.READ_WRITE) {
            check.check(validAdmin);
            check.check(validWrite);
            try {
                check.check(validRead);
                Assert.fail("Should not have been able to read/write with admin permissions");
            } catch (final Exception e) {
                // Expected
            }
        } else if (requiredRole == UserContext.ROLE.ADMIN) {
            try {
                check.check(validRead);
                Assert.fail("Should not have been able to read/write with admin permissions");
            } catch (final Exception e) {
                // Expected
            }
            try {
                check.check(validWrite);
                Assert.fail("Should not have been able to read/write with admin permissions");
            } catch (final Exception e) {
                // Expected
            }
            check.check(validAdmin);
        }
    }

    @Test
    public void testRbacJwtIssueToken() {
        checkPermissions(UserContext.ROLE.ADMIN, this::adminRbacJwtIssueToken);
    }

    @Test
    public void testListHeaders() {
        checkPermissions(UserContext.ROLE.READ, this::adminIndexListHeaders);
    }

    @Test
    public void testCardinalityIndex() {
        checkPermissions(UserContext.ROLE.READ, this::adminIndexCardinality);
    }

    @Test
    public void testCreateIndex() {
        checkPermissions(UserContext.ROLE.ADMIN, this::adminIndexCreate);
    }

    @Test
    public void testDropIndex() {
        checkPermissions(UserContext.ROLE.ADMIN, this::adminIndexDrop);
    }

    @Test
    public void testStatusIndex() {
        checkPermissions(UserContext.ROLE.READ, this::adminIndexStatus);
    }

    @Test
    public void testMetadataSummary() {
        checkPermissions(UserContext.ROLE.READ, this::adminMetadataSummary);
    }

    @Test
    public void testMetadataUsage() {
        checkPermissions(UserContext.ROLE.READ, this::adminMetadataUsage);
    }

    @BeforeClass
    public static void setup() {
        server = FireflyServer.start(new String[]{"../conf/credentials-config/gremlin-server-authenticator.yaml"});
    }

    @AfterClass
    public static void teardown() {
        server.stop().join();
    }
}
