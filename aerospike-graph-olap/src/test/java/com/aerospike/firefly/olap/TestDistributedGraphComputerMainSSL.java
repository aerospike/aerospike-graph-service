package com.aerospike.firefly.olap;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
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
            .sign(Algorithm.HMAC256("lyndon_secret"));

    final static String noRole = JWT.create()
            .withSubject("lyndon_username")
            .withIssuer("aerospike")
            .sign(Algorithm.HMAC256("lyndon_secret"));

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
