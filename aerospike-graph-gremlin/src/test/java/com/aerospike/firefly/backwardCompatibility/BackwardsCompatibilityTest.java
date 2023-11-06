package com.aerospike.firefly.backwardCompatibility;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.DockerUtil;
import com.aerospike.firefly.util.VersionUtil;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.DockerUtil.AEROSPIKE_GRAPH_SERVICE;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
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
    public void testVersionCompatibility() {
        final GraphTraversalSource g2 = traversal().withRemote(DriverRemoteConnection.using("localhost", port, "g"));
        g2.V().drop().iterate();
        final Vertex lyndon = g2.addV("person").property("name", "Lyndon").next();
        final Vertex simon = g2.addV("person").property("name", "Simon").next();
        final Vertex grant = g2.addV("person").property("name", "Grant").next();
        final Vertex joe = g2.addV("person").property("name", "joe").next();
        final Vertex rahul = g2.addV("person").property("name", "Rahul").next();
        final Vertex ishaan = g2.addV("person").property("name", "Ishaan").next();

        // Try adding different types of properties.
        g2.V(lyndon.id()).property("isDope", true).iterate();
        g2.V(simon.id()).property("isDope", "true").property("foo", List.of("baz")).iterate();
        g2.V(grant.id()).property("isDope", 1).iterate();
        g2.V(joe.id()).property("isDope", 1.0).iterate();
        g2.V(rahul.id()).property("isDope", 1.0).iterate();
        g2.V(ishaan.id()).property("isDope", 1L).iterate();

        // Create some edges from lyndon to everyone.
        g2.addE("knows").from(lyndon).to(simon).property("foo", "bar").iterate();
        g2.addE("knows").from(lyndon).to(grant).property("foo", false).property("foo", List.of("baz")).iterate();
        g2.addE("knows").from(lyndon).to(joe).property("foo", 25).iterate();
        g2.addE("knows").from(lyndon).to(rahul).property("foo", 25.0).iterate();
        g2.addE("knows").from(lyndon).to(ishaan).property("foo", 25.0).iterate();

        // Create an edge from simon to everyone.
        g2.addE("knows").from(simon).to(lyndon).property("foo", 25L).iterate();
        g2.addE("knows").from(simon).to(grant).iterate();
        g2.addE("knows").from(simon).to(joe).property("foo", List.of("baz")).iterate();
        g2.addE("knows").from(simon).to(rahul).iterate();
        g2.addE("knows").from(simon).to(ishaan).iterate();

        // Create two graph traversal sources, one for the local graph and one for the remote graph.
        final GraphTraversalSource g1 = graph.traversal();

        // Assert that the vertices are the same.
        Assert.assertEquals(g1.V().count().next(), g2.V().count().next());
        final List<Object> vertices1 = g1.V().order().by("name").id().toList();
        final List<Object> vertices2 = g2.V().order().by("name").id().toList();
        Assert.assertEquals(vertices1.size(), vertices2.size());
        Assert.assertEquals(vertices1, vertices2);
        for (int i = 0; i < vertices1.size(); i++) {
            final Map<Object, Object> vertex1Properties = g1.V(vertices1.get(i)).elementMap().next();
            final Map<Object, Object> vertex2Properties = g2.V(vertices2.get(i)).elementMap().next();
            Assert.assertEquals(vertex1Properties, vertex2Properties);

            final List<Object> outEdges1 = g1.V(vertices1.get(i)).outE().order().by(T.id).id().toList();
            final List<Object> outEdges2 = g2.V(vertices2.get(i)).outE().order().by(T.id).id().toList();
            Assert.assertEquals(outEdges1.size(), outEdges2.size());
            Assert.assertEquals(outEdges1, outEdges2);

            for (int j = 0; j < outEdges1.size(); j++) {
                final Map<Object, Object> edge1Properties = g1.E(outEdges1.get(j)).elementMap().next();
                final Map<Object, Object> edge2Properties = g2.E(outEdges2.get(j)).elementMap().next();
                Assert.assertEquals(edge1Properties, edge2Properties);

                final Object inVId1 = g1.E(outEdges1.get(j)).inV().id().next();
                final Object inVId2 = g2.E(outEdges2.get(j)).inV().id().next();
                Assert.assertEquals(inVId1, inVId2);

                final Object outVId1 = g1.E(outEdges1.get(j)).outV().id().next();
                final Object outVId2 = g2.E(outEdges2.get(j)).outV().id().next();
                Assert.assertEquals(outVId1, outVId2);
            }

            final List<Object> inEdges1 = g1.V(vertices1.get(i)).inE().order().by(T.id).id().toList();
            final List<Object> inEdges2 = g2.V(vertices2.get(i)).inE().order().by(T.id).id().toList();
            Assert.assertEquals(inEdges1.size(), inEdges2.size());
            Assert.assertEquals(inEdges1, inEdges2);

            for (int j = 0; j < inEdges1.size(); j++) {
                final Map<Object, Object> edge1Properties = g1.E(inEdges1.get(j)).elementMap().next();
                final Map<Object, Object> edge2Properties = g2.E(inEdges2.get(j)).elementMap().next();
                Assert.assertEquals(edge1Properties, edge2Properties);

                final Object inVId1 = g1.E(inEdges1.get(j)).inV().id().next();
                final Object inVId2 = g2.E(inEdges2.get(j)).inV().id().next();
                Assert.assertEquals(inVId1, inVId2);

                final Object outVId1 = g1.E(inEdges1.get(j)).outV().id().next();
                final Object outVId2 = g2.E(inEdges2.get(j)).outV().id().next();
                Assert.assertEquals(outVId1, outVId2);
            }
        }

        Assert.assertEquals(g1.E().count().next(), g2.E().count().next());
        final List<Edge> edges1 = g1.E().order().by(T.id).toList();
        final List<Edge> edges2 = g2.E().order().by(T.id).toList();

        Assert.assertEquals(edges1.size(), edges2.size());
        for (int j = 0; j < edges1.size(); j++) {
            Assert.assertEquals(edges1.get(j).id(), edges2.get(j).id());
            Assert.assertEquals(edges1.get(j).label(), edges2.get(j).label());
            Assert.assertEquals(edges1.get(j).inVertex().id(), edges2.get(j).inVertex().id());
            Assert.assertEquals(edges1.get(j).outVertex().id(), edges2.get(j).outVertex().id());
        }
    }

    @Before
    public void beforeEachTest() {
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

        // Load a single vertex into the graph and drop it. This will force the graph to write it's data model version.
        final GraphTraversalSource g = traversal().withRemote(
                DriverRemoteConnection.using("localhost", port, "g"));
        try {
            Thread.sleep(100);
        } catch (final InterruptedException e) {
            fail("Failed to sleep for 100ms");
        }
    }

    @After
    public void afterEachTest() {
        if (graph != null) {
            graph.close();
            graph = null;
        }

        // Cleanup any dangling containers (catch all for test issues).
        dockerUtil.stopAllDockerImages();
    }
}
