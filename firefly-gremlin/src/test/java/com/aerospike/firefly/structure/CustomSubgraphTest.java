package com.aerospike.firefly.structure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirements;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(GremlinProcessRunner.class)
public abstract class CustomSubgraphTest extends AbstractGremlinProcessTest {
    public CustomSubgraphTest() {
    }

    public abstract Traversal<Vertex, Graph> get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(final Object v1Id, final Graph subgraph);

    public abstract GraphTraversal<Vertex, Object> get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(final Graph subgraph);

    public abstract Traversal<Vertex, Vertex> get_g_withSideEffectXsgX_V_hasXname_danielX_outE_subgraphXsgX_inV(final Graph subgraph);

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirements({@FeatureRequirement(
            featureClass = Graph.Features.VertexFeatures.class,
            feature = "AddVertices"
    ), @FeatureRequirement(
            featureClass = Graph.Features.EdgeFeatures.class,
            feature = "AddEdges"
    ), @FeatureRequirement(
            featureClass = Graph.Features.VertexFeatures.class,
            feature = "UserSuppliedIds"
    ), @FeatureRequirement(
            featureClass = Graph.Features.EdgeFeatures.class,
            feature = "UserSuppliedIds"
    )})
    public void g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX() throws Exception {
        Configuration config = this.graphProvider.newGraphConfiguration("subgraph", this.getClass(), this.name.getMethodName(), GraphData.MODERN);
        this.graphProvider.clear(config);
        Graph subgraph = this.graphProvider.openTestGraph(config);
        Traversal<Vertex, Graph> traversal = this.get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(this.convertToVertexId("marko"), subgraph);
        this.printTraversalForm(traversal);
        subgraph = (Graph)traversal.next();
        assertVertexEdgeCounts(subgraph, 3, 2);
        subgraph.edges(new Object[0]).forEachRemaining((e) -> {
            Assert.assertEquals("knows", e.label());
            Assert.assertEquals("marko", this.g.E(new Object[]{e}).outV().values(new String[]{"name"}).next());
            Assert.assertEquals(new Integer(29), this.g.E(new Object[]{e}).outV().values(new String[]{"age"}).next());
            Assert.assertEquals("person", this.g.E(new Object[]{e}).outV().label().next());
            String name = (String)this.g.E(new Object[]{e}).inV().values(new String[]{"name"}).next();
            if (name.equals("vadas")) {
                Assert.assertEquals(0.5, (Double)this.g.E(new Object[]{e}).values(new String[]{"weight"}).next(), 1.0E-4);
            } else if (name.equals("josh")) {
                Assert.assertEquals(1.0, (Double)this.g.E(new Object[]{e}).values(new String[]{"weight"}).next(), 1.0E-4);
            } else {
                Assert.fail("There's a vertex present that should not be in the subgraph");
            }

        });
        this.graphProvider.clear(subgraph, config);
    }

    @Test
    @LoadGraphWith(GraphData.MODERN)
    @FeatureRequirements({@FeatureRequirement(
            featureClass = Graph.Features.VertexFeatures.class,
            feature = "AddVertices"
    ), @FeatureRequirement(
            featureClass = Graph.Features.EdgeFeatures.class,
            feature = "AddEdges"
    ), @FeatureRequirement(
            featureClass = Graph.Features.VertexFeatures.class,
            feature = "UserSuppliedIds"
    ), @FeatureRequirement(
            featureClass = Graph.Features.EdgeFeatures.class,
            feature = "UserSuppliedIds"
    )})
    public void g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup() throws Exception {
        System.out.println("this.graphProvider.newGraphConfiguration(\"subgraph\", this.getClass(), this.name.getMethodName(), GraphData.MODERN);");
        Configuration config = this.graphProvider.newGraphConfiguration("subgraph", this.getClass(), this.name.getMethodName(), GraphData.MODERN);
        System.out.println("this.graphProvider.clear(config)");
        this.graphProvider.clear(config);
        System.out.println("this.graphProvider.openTestGraph(config)");
        Graph subgraph = this.graphProvider.openTestGraph(config);
        System.out.println("this.get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(subgraph);");
        System.out.println("Reload: " + subgraph.traversal().V().toList() + "====" + subgraph.traversal().E().toList());
        GraphTraversal<Vertex, Object> traversal = this.get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(subgraph);
        System.out.println(traversal.toString());
        List<Vertex> vertices = subgraph.traversal().V().toList();
        System.out.println(vertices);


        Graph subgraph2 = this.graphProvider.openTestGraph(config);
        System.out.println("subraph2==> " + this.g.withSideEffect("sg", () -> subgraph2).V().toList());
        this.g.withSideEffect("sg", () -> subgraph2).V().values("name").dedup();
        System.out.println("subraph2==> " + this.g.withSideEffect("sg", () -> subgraph2).V().toList());


        checkResults(Arrays.asList("marko", "josh", "peter"), traversal);
        subgraph = (Graph)traversal.asAdmin().getSideEffects().get("sg");
        assertVertexEdgeCounts(subgraph, 5, 4);
        this.graphProvider.clear(subgraph, config);
    }

    @Test
    @LoadGraphWith(GraphData.CREW)
    @FeatureRequirements({@FeatureRequirement(
            featureClass = Graph.Features.VertexFeatures.class,
            feature = "AddVertices"
    ), @FeatureRequirement(
            featureClass = Graph.Features.EdgeFeatures.class,
            feature = "AddEdges"
    ), @FeatureRequirement(
            featureClass = Graph.Features.VertexFeatures.class,
            feature = "UserSuppliedIds"
    ), @FeatureRequirement(
            featureClass = Graph.Features.EdgeFeatures.class,
            feature = "UserSuppliedIds"
    )})
    public void g_withSideEffectXsgX_V_hasXname_danielXout_capXsgX() throws Exception {
        Configuration config = this.graphProvider.newGraphConfiguration("subgraph", this.getClass(), this.name.getMethodName(), GraphData.CREW);
        this.graphProvider.clear(config);
        Graph subgraph = this.graphProvider.openTestGraph(config);
        Traversal<Vertex, Vertex> traversal = this.get_g_withSideEffectXsgX_V_hasXname_danielX_outE_subgraphXsgX_inV(subgraph);
        this.printTraversalForm(traversal);
        traversal.iterate();
        assertVertexEdgeCounts(subgraph, 3, 2);
        List<Object> locations = subgraph.traversal().V(new Object[0]).has("name", "daniel").values(new String[]{"location"}).toList();
        MatcherAssert.assertThat(locations, Matchers.contains(new String[]{"spremberg", "kaiserslautern", "aachen"}));
        this.graphProvider.clear(subgraph, config);
    }

    public static class Traversals extends CustomSubgraphTest {
        public Traversals() {
        }

        public Traversal<Vertex, Graph> get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(final Object v1Id, final Graph subgraph) {
            return this.g.withSideEffect("sg", () -> {
                return subgraph;
            }).V(new Object[]{v1Id}).outE(new String[]{"knows"}).subgraph("sg").values(new String[]{"name"}).cap("sg", new String[0]);
        }

        public GraphTraversal<Vertex, Object> get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(final Graph subgraph) {
            return this.g.withSideEffect("sg", () -> {
                return subgraph;
            }).V(new Object[0]).repeat(__.bothE(new String[]{"created"}).subgraph("sg").outV()).times(5).values(new String[]{"name"}).dedup(new String[0]);
        }

        public Traversal<Vertex, Vertex> get_g_withSideEffectXsgX_V_hasXname_danielX_outE_subgraphXsgX_inV(final Graph subgraph) {
            return this.g.withSideEffect("sg", () -> {
                return subgraph;
            }).V(new Object[0]).has("name", "daniel").outE(new String[0]).subgraph("sg").inV();
        }
    }
}
