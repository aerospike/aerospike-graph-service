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

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.fail;

public class TestDistributedGraphComputerMainSSL {

    private Cluster getCluster(final String jwt, boolean ssl) {
        Cluster.Builder builder = Cluster.build().addContactPoint("localhost").port(8182);
        if (ssl) {
            builder.enableSsl(true).sslSkipCertValidation(true);
        }
        if (jwt != null) {
            builder.credentials("lyndon_username", jwt);
        }
        return builder.create();
    }

    final static String validRead = JWT.create()
            .withClaim("role", "READ")
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));

    final static String noRole = JWT.create()
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("your-jwt-secret-here"));

    @Test
    public void testDistributedGraphComputerMainGLVSSL() throws Exception {
        DistributedGraphComputerMain.main(new String[]{
                "-local",
                "-c", "src/test/resources/config/test-gremlin-server-main-glv-ssl.properties"
        });

        try (final DriverRemoteConnection drc = DriverRemoteConnection.using(getCluster(null, true))) {
            traversal().withRemote(drc).withComputer().V().count().next();
        }
        try (final DriverRemoteConnection drcWithoutSSL = DriverRemoteConnection.using(getCluster(null, false))) {
            traversal().withRemote(drcWithoutSSL).withComputer().V().count().next();
            fail("Should not have been able to connect without SSL.");
        } catch (final Exception ignored) {
        }
        DistributedGraphComputerMain.fireflyServerForTesting.stop();
    }

    @Test
    @Ignore
    public void testDistributedGraphComputerMainGLVSSLJWT() throws Exception {
        DistributedGraphComputerMain.main(new String[]{
                "-local",
                "-c", "src/test/resources/config/test-gremlin-server-main-glv-ssl-jwt.properties"
        });

        try (final DriverRemoteConnection drc = DriverRemoteConnection.using(getCluster(validRead, true))) {
            traversal().withRemote(drc).withComputer().V().count().next();
        }
        try (final DriverRemoteConnection drcWithoutSSL = DriverRemoteConnection.using(getCluster(noRole, true))) {
            traversal().withRemote(drcWithoutSSL).withComputer().V().count().next();
            fail("Should not have been able to connect without role.");
        } catch (final Exception ignored) {
        }
        DistributedGraphComputerMain.fireflyServerForTesting.stop();
    }
}
