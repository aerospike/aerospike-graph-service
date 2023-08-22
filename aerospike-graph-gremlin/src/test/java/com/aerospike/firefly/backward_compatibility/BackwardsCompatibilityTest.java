package com.aerospike.firefly.backward_compatibility;


import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.DockerUtil;
import com.aerospike.firefly.util.VersionUtil;
import io.cucumber.java.sl.In;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.DockerUtil.AEROSPIKE_GRAPH_SERVICE;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.junit.Assert.fail;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class BackwardsCompatibilityTest {
    private static FireflyGraph graph;
    private static final DockerUtil dockerUtil = new DockerUtil();
    private static int port;

    @Test
    public void testBackwardsCompatibilityVersioning() {
        final String versionString = graph.getBaseGraph().getDataModelMetadata().getDataModelVersion().toString();
        final VersionUtil versionUtil = new VersionUtil(versionString);
        final String testVersion = System.getenv("COMPATIBILITY_VERSION");

        // Assert version written to aerospike is the version set in the pom.xml.
        Assert.assertEquals(testVersion, versionString);
        final String codeVersion = FireflyGraph.FIREFLY_VERSION;

        final VersionUtil testVersionUtil = new VersionUtil(codeVersion);

        // Validate that the major versions are equal.
        Assert.assertEquals(testVersionUtil.getMajor(), versionUtil.getMajor());

        // Validate that either the minor or patch versions are different.
        Assert.assertTrue((testVersionUtil.getMinor() != versionUtil.getMinor() || testVersionUtil.getPatch() != versionUtil.getPatch()));
    }

    @Test
    public void testVersionCompatibilityConcurrent() {
        // Create two graph traversal sources, one for the local graph and one for the remote graph.
        final GraphTraversalSource g1 = graph.traversal();
        final GraphTraversalSource g2 = traversal().withRemote(DriverRemoteConnection.using("localhost", port, "g"));

        final Vertex v1_1 = g1.addV("person").property("name", "Lyndon1").next();
        final Vertex v2_1 = g2.addV("person").property("name", "Lyndon2").next();

        final Vertex v1_2 = g2.V(v1_1.id()).next();
        final Vertex v2_2 = g1.V(v2_1.id()).next();

        Assert.assertEquals(v1_1.id(), v1_2.id());
        Assert.assertEquals(v2_1.id(), v2_2.id());

        g2.V(v1_1.id()).property("age", 30).iterate();
        g1.V(v2_1.id()).property("age", 30).iterate();

        g1.V(v1_1.id()).property("occupation", "SDE").iterate();
        g2.V(v2_1.id()).property("occupation", "SDE").iterate();

        final Set<?> g1Properties_v1 = g1.V(v1_1.id()).values("age", "occupation").toSet();
        final Set<?> g2Properties_v1 = g2.V(v1_1.id()).values("age", "occupation").toSet();

        final Set<?> g1Properties_v2 = g1.V(v2_1.id()).values("age", "occupation").toSet();
        final Set<?> g2Properties_v2 = g2.V(v2_1.id()).values("age", "occupation").toSet();

        Assert.assertEquals(g1Properties_v1, g2Properties_v1);
        Assert.assertEquals(g1Properties_v2, g2Properties_v2);
    }

    @Test
    public void testVersionCompatibilityUpgrade() {
        // This test will test that a graph written by a previous version can be loaded by a graph with a newer version.
    }

    @BeforeClass
    public static void beforeClass() {
        graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));
        final String versionString = graph.getBaseGraph().getDataModelMetadata().getDataModelVersion().toString();
        graph.traversal().V().drop().iterate();
        final VersionUtil versionUtil = new VersionUtil(versionString);
        final String testVersion = System.getenv("COMPATIBILITY_VERSION");

        // If testVersion is not set and the minor/patch are non-zero, fail the test.
        if ((testVersion == null || testVersion.isEmpty()) && (versionUtil.getMinor() != 0 || versionUtil.getPatch() != 0)) {
            fail("COMPATIBILITY_VERSION environment variable not set. All versions that are not X.0.0 MUST have a " +
                    "COMPATIBILITY_VERSION environment variable set to test backwards compatibility. This can be set " +
                   "in the root pom.xml");
        } else {
            // If testVersion is not set and the minor/patch are zero, skip the test.
            Assume.assumeFalse("COMPATIBILITY_VERSION environment variable not set. Skipping backwards compatibility " +
                            "test because this is the first of this major version.",
                    testVersion == null || testVersion.isEmpty());
        }

        // Boot the docker image, we are good to test.
        port = dockerUtil.startDockerImage(AEROSPIKE_GRAPH_SERVICE, testVersion);
    }

    @AfterClass
    public static void afterClass() {
        if (graph != null) {
            graph.close();
            graph = null;
        }

        // Cleanup any dangling containers (catch all for test issues).
        dockerUtil.stopAllDockerImages();
    }

    @Test
    public void test() throws ParseException {
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("localhost", 8182, "g"));
        Instant start = Instant.now();
        BulkSet pageViews = g.with("evaluationTimeout", 30 * 60 * 1000).
                V("15secs_pageview").
                as("start_event").
                in("object").where(
                        __.values("time").is(
                                between(
                                        d("2023-04-17T00:00:00Z"), d("2023-07-17T23:59:00Z")))).
                as("15secs_pageviews").
                count().
                toBulkSet();

        BulkSet<?> result =
                g.with("evaluationTimeout", 30 * 60 * 1000).
                V("formFieldEntered").
                        as("start_event").
                in("object").where(__.values("time").is(between(
                        d("2023-04-17T00:00:00Z"),
                        d("2023-07-17T23:59:00Z")))).
                as("firstv").
                repeat(__.timeLimit(5 * 60  * 1000). // Can set a time limit here to break out when we have reached the time limit.
                        in("prev").hasLabel("visit")).
                emit(__.as("lastv").
                        and(
                                __.loops().is(lt(10)), // Can limit the loops() here to break out when we have all paths of a certain depth.
                                __.in("prev").count().is(neq(0)),
                                __.values("time").
                                        is(between(
                                                d("2023-04-17T00:00:00Z"),
                                                d("2023-07-17T23:59:00Z"))),
                                __.
                                        select("firstv").limit(1).values("time").as("firstTime").
                                        select("lastv").limit(1).values("time").as("lastTime").
                                        math("lastTime - firstTime").
                                        is(gt(0))
                        )).
                project("a", "b").
                by(
                        __.path().by(T.id). // Switched from "id" to T.id because we switched the id.
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event").
                                by("event").by("event").by("event").by("event").by("event")
                ).
                by(__.path().unfold().count()).
                order().by("b").toBulkSet();
        Instant end = Instant.now();
        System.out.println("Time taken: " + Duration.between(start, end).toMillis() + "ms");
        System.out.println(result);
    }


    long d(final String date) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(date).getTime() / 1000;
    }


}
