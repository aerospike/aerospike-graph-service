package com.aerospike.firefly.security;

import com.aerospike.firefly.runtime.FireflyServer;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class JwtAuthenticationFireflyServerTest {
    private static FireflyServer server;

    final String validAdmin = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String foo = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("kenny")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String validWrite = JWT.create()
            .withClaim("role", "WRITE")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String validRead = JWT.create()
            .withClaim("role", "READ")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String invalidUser = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("invalid_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String invalidIssuer = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("lyndon_username")
            .withIssuer("invalid_aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String invalidRole = JWT.create()
            .withClaim("role", "ROLE")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String nullUsername = JWT.create()
            .withClaim("role", "ADMIN")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String nullIssuer = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("lyndon_username")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String nullRole = JWT.create()
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));
    final String invalidSecret = JWT.create()
            .withClaim("role", "READ")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("invalid_secret"));

    @BeforeClass
    public static void setup() {
        server = FireflyServer.main(new String[]{"../conf/credentials-config/gremlin-server-authenticator.yaml"});
    }

    @AfterClass
    public static void teardown() {
        server.stop().join();
    }

    private static GraphTraversalSource getGraphTraversalSource(final String username, final String jwt) {
        final Cluster cluster = Cluster.build()
                .addContactPoint("localhost").port(8182)
                .credentials(username, jwt)
                .create();
        final DriverRemoteConnection drc = DriverRemoteConnection.using(cluster, "g");
        return traversal().withRemote(drc);
    }

    @Test
    public void testServerAuthInvalidUserMatch() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", invalidUser);
        Assert.assertThrows(
                "User does not match token subject",
                RuntimeException.class, () -> g.V().toList());
    }

    @Test
    public void testServerAuthInvalidUserMismatch2() {
        final GraphTraversalSource g = getGraphTraversalSource("invalid_username", validRead);
        Assert.assertThrows(
                "User does not match token subject",
                RuntimeException.class, () -> g.V().toList());
    }

    @Test
    public void testServerAuthInvalidIssuer() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", invalidIssuer);
        Assert.assertThrows(
                "Failed to authenticate",
                CompletionException.class, () -> g.V().toList());
    }

    @Test
    public void testServerAuthInvalidSecret() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", invalidSecret);
        Assert.assertThrows(
                "Failure to validate credentials: The Token's Signature resulted invalid when verified using the Algorithm: HmacSHA256",
                CompletionException.class, () -> g.V().toList());
    }

    @Test
    public void testServerAuthInvalidRole() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", invalidRole);
        Assert.assertThrows(
                "User does not have write access.",
                RuntimeException.class, () -> g.V().toList());
    }

    @Test
    public void testServerAuthNullUser() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", nullUsername);
        Assert.assertThrows(
                "User does not match token subject",
                CompletionException.class, () -> g.V().toList());
    }

    @Test
    public void testServerAuthNullIssuer() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", nullIssuer);
        Assert.assertThrows(
                "Failure to validate credentials: The Claim 'iss' is not present in the JWT.",
                CompletionException.class, () -> g.V().toList());
    }

    @Test
    public void testServerAuthNullRole() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", nullRole);
        Assert.assertThrows(
                "User does not have read access.",
                RuntimeException.class, () -> g.V().toList());
    }

    @Test
    public void testReadOnly() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", validRead);
        g.V().toList();
        Assert.assertThrows(
                "User does not have write access.",
                RuntimeException.class, () -> g.addV().iterate());
        Assert.assertThrows(
                "User does not have admin access.",
                RuntimeException.class, () -> g.call("aerospike.graph.admin.index.drop").
                        with("element_type", "vertex").with("property_key", "~label").next());
    }

    @Test
    public void testWriteOnly() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", validWrite);
        g.V().toList();
        g.addV().iterate();
        Assert.assertThrows(
                "User does not have admin access.",
                RuntimeException.class, () -> g.call("aerospike.graph.admin.index.drop").
                        with("element_type", "vertex").with("property_key", "~label").next());
    }

    @Test
    public void testAdminOnly() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", validAdmin);
        g.V().toList();
        g.addV().iterate();
        g.call("aerospike.graph.admin.index.drop").with("element_type", "vertex").with("property_key", "~label").next();
    }
}
