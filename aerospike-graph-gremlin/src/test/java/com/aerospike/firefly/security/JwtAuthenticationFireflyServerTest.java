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

package com.aerospike.firefly.security;

import com.aerospike.firefly.runtime.FireflyServer;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class JwtAuthenticationFireflyServerTest {
    private static FireflyServer server;

    final String validAdmin = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));

    final String validGraph0Admin = JWT.create()
            // admin for graph0 and no role for graph1
            .withClaim("role", Map.of("0","ADMIN"))
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));

    final String validGraph0ReadGraph1Admin = JWT.create()
            // read for graph0 and admin for graph1
            .withClaim("role", Map.of("0","READ", "1", "ADMIN"))
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));

    final String validGraph1Admin = JWT.create()
            // read for graph0 and admin for graph1
            .withClaim("role", Map.of("1", "ADMIN"))
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));

    final String validWrite = JWT.create()
            .withClaim("role", "READ_WRITE")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));
    final String validRead = JWT.create()
            .withClaim("role", "READ")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));
    final String invalidUser = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("invalid_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));
    final String invalidIssuer = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("lyndon_username")
            .withIssuer("invalid_aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));
    final String invalidRole = JWT.create()
            .withClaim("role", "ROLE")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));
    final String nullUsername = JWT.create()
            .withClaim("role", "ADMIN")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));
    final String nullIssuer = JWT.create()
            .withClaim("role", "ADMIN")
            .withSubject("lyndon_username")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));
    final String nullRole = JWT.create()
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));
    final String invalidSecret = JWT.create()
            .withClaim("role", "READ")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("invalid_secret"));
    final String invalidRole2 = JWT.create()
            .withClaim("role", "WRITE21")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));

    @BeforeClass
    public static void setup() {
        server = FireflyServer.start(new String[]{"../conf/credentials-config/gremlin-server-authenticator.yaml"});
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

    private static Client getClient(final String username, final String jwt) {
        final Cluster cluster = Cluster.build()
                .addContactPoint("localhost").port(8182)
                .credentials(username, jwt)
                .create();
        return cluster.connect();
    }

    @Test
    public void testInvalidUserMatch() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", invalidUser);
        Assert.assertThrows(
                "User does not match token subject",
                RuntimeException.class, () -> g.V().toList());
    }

    @Test
    public void testScriptInvalidUserMatch() {
        final Client client = getClient("lyndon_username", invalidUser);
        Assert.assertThrows(
                "User does not match token subject",
                RuntimeException.class, () ->  readQuery(client));
    }

    @Test
    public void testInvalidUserMismatch2() {
        final GraphTraversalSource g = getGraphTraversalSource("invalid_username", validRead);
        Assert.assertThrows(
                "User does not match token subject",
                RuntimeException.class, () -> g.V().toList());
    }

    @Test
    public void testScriptInvalidUserMismatch2() {
        final Client client = getClient("invalid_username", validRead);
        Assert.assertThrows(
                "User does not match token subject",
                RuntimeException.class, () -> readQuery(client));
    }

    @Test
    public void testInvalidIssuer() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", invalidIssuer);
        Assert.assertThrows(
                "Failed to authenticate",
                CompletionException.class, () -> g.V().toList());
    }

    @Test
    public void testScriptInvalidIssuer() {
        final Client client = getClient("lyndon_username", invalidIssuer);
        Assert.assertThrows(
                "Failed to authenticate",
                CompletionException.class, () -> readQuery(client));
    }

    @Test
    public void testInvalidSecret() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", invalidSecret);
        Assert.assertThrows(
                "Failure to validate credentials: The Token's Signature resulted invalid when verified using the Algorithm: HmacSHA256",
                CompletionException.class, () -> g.V().toList());
    }

    @Test
    public void testScriptInvalidSecret() {
        final Client client = getClient("lyndon_username", invalidSecret);
        Assert.assertThrows(
                "Failure to validate credentials: The Token's Signature resulted invalid when verified using the Algorithm: HmacSHA256",
                CompletionException.class, () -> readQuery(client));
    }

    @Test
    public void testInvalidRole() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", invalidRole);
        Assert.assertThrows(
                "User does not have write access.",
                RuntimeException.class, () -> g.V().toList());
    }

    @Test
    public void testScriptInvalidRole() {
        final Client client = getClient("lyndon_username", invalidRole);
        Assert.assertThrows(
                "User does not have write access.",
                RuntimeException.class, () -> readQuery(client));
    }

    @Test
    public void testNullUser() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", nullUsername);
        Assert.assertThrows(
                "User does not match token subject",
                CompletionException.class, () -> g.V().toList());
    }

    @Test
    public void testScriptNullUser() {
        final Client client = getClient("lyndon_username", nullUsername);
        Assert.assertThrows(
                "User does not match token subject",
                CompletionException.class, () -> readQuery(client));
    }

    @Test
    public void testNullIssuer() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", nullIssuer);
        Assert.assertThrows(
                "Failure to validate credentials: The Claim 'iss' is not present in the JWT.",
                CompletionException.class, () -> g.V().toList());
    }
    @Test
    public void testScriptNullIssuer() {
        final Client client = getClient("lyndon_username", nullIssuer);
        Assert.assertThrows(
                "Failure to validate credentials: The Claim 'iss' is not present in the JWT.",
                CompletionException.class, () -> readQuery(client));
    }

    @Test
    public void testInvalidRole2() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", invalidRole2);
        Assert.assertThrows(
                "User does not have read access.",
                RuntimeException.class, () -> g.V().toList());
    }

    @Test
    public void testScriptInvalidRole2() {
        final Client client = getClient("lyndon_username", invalidRole2);
        Assert.assertThrows(
                "User does not have read access.",
                RuntimeException.class, () -> readQuery(client));
    }

    @Test
    public void testNullRole() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", nullRole);
        Assert.assertThrows(
                "User does not have read access.",
                RuntimeException.class, () -> g.V().toList());
    }

    @Test
    public void testScriptNullRole() {
        final Client client = getClient("lyndon_username", nullRole);
        Assert.assertThrows(
                "User does not have read access.",
                RuntimeException.class, () -> readQuery(client));
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
    public void testReadOnlyMultiTenant() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", validGraph0ReadGraph1Admin);
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
    public void testReadOnlyNoRoleMultiTenant() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", validGraph1Admin);
        Assert.assertThrows(
                "User does not have a valid role.",
                RuntimeException.class, () -> g.V().toList());
        Assert.assertThrows(
                "User does not have a valid role.",
                RuntimeException.class, () -> g.addV().iterate());
        Assert.assertThrows(
                "User does not have a valid role.",
                RuntimeException.class, () -> g.call("aerospike.graph.admin.index.drop").
                        with("element_type", "vertex").with("property_key", "~label").next());
    }

    @Test
    public void testScriptReadOnly() {
        final Client client = getClient("lyndon_username", validRead);
        readQuery(client);
        Assert.assertThrows(
                "User does not have write access.",
                RuntimeException.class, () -> writeQuery(client));
        Assert.assertThrows(
                "User does not have admin access.",
                RuntimeException.class, () -> callQuery(client));
    }

    @Test
    public void testScriptReadOnlyMultiTenant() {
        final Client client = getClient("lyndon_username", validGraph0ReadGraph1Admin);
        readQuery(client);
        Assert.assertThrows(
                "User does not have write access.",
                RuntimeException.class, () -> writeQuery(client));
        Assert.assertThrows(
                "User does not have admin access.",
                RuntimeException.class, () -> callQuery(client));
    }

    @Test
    public void testScriptReadOnlyNoRoleMultiTenant() {
        final Client client = getClient("lyndon_username", validGraph1Admin);
        Assert.assertThrows(
                "User does not have a valid role.",
                RuntimeException.class, () -> readQuery(client));
        Assert.assertThrows(
                "User does not have a valid role.",
                RuntimeException.class, () -> writeQuery(client));
        Assert.assertThrows(
                "User does not have admin access.",
                RuntimeException.class, () -> callQuery(client));
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
    public void testScriptWriteOnly() {
        final Client client = getClient("lyndon_username", validWrite);
        readQuery(client);
        writeQuery(client);
        Assert.assertThrows(
                "User does not have admin access.",
                RuntimeException.class, () -> callQuery(client));
    }

    @Test
    public void testAdminOnly() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", validAdmin);
        g.V().toList();
        g.addV().iterate();
        g.call("aerospike.graph.admin.index.drop").with("element_type", "vertex").with("property_key", "~label").next();
    }

    @Test
    public void testAdminMultiTenant() {
        final GraphTraversalSource g = getGraphTraversalSource("lyndon_username", validGraph0Admin);
        g.V().toList();
        g.addV().iterate();
        g.call("aerospike.graph.admin.index.drop").with("element_type", "vertex").with("property_key", "~label").next();
    }


    @Test
    public void testScriptAdminOnly() {
        final Client client = getClient("lyndon_username", validAdmin);
        readQuery(client);
        writeQuery(client);
        callQuery(client);
    }

    @Test
    public void testScriptAdminOnlyMultiTenant() {
        final Client client = getClient("lyndon_username", validGraph0Admin);
        readQuery(client);
        writeQuery(client);
        callQuery(client);
    }

    private void readQuery(Client client) {
        client.submit("g.V().toList()").all().join();
        client.submit("g.V()").all().join();
    }

    private void writeQuery(Client client) {
        client.submit("g.addV().iterate()").all().join();
        client.submit("g.addV()").all().join();
    }

    private void callQuery(Client client) {
        client.submit(
                "g.call(\"aerospike.graph.admin.index.drop\").with(\"element_type\", \"vertex\").with(\"property_key\", \"~label\").next()"
        ).all().join();
        client.submit(
                "g.call(\"aerospike.graph.admin.index.drop\").with(\"element_type\", \"vertex\").with(\"property_key\", \"~label\")"
        ).all().join();
    }
}
