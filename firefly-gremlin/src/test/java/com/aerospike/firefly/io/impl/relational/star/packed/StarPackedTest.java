package com.aerospike.firefly.io.impl.relational.star.packed;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class StarPackedTest extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return false;
    }

    @Override
    protected boolean runTest() {
        // Only run if the StarPackedGraph is being used.
        return graph.getDataModel().equals(StarPackedGraph.DATA_MODEL);
    }

    @Before
    public void beforeEachTest() throws InterruptedException {
        // Use this to clear data here because we haven't solidified our deletes yet.
        graph.getBaseGraph().dropDatabase();
        Thread.sleep(1000);
    }

    @Test
    public void testStarPackedGraphWrite() {
        final GraphTraversalSource g = graph.traversal();
        final AerospikeConnection db = graph.getBaseGraph();

        // Insert 5 vertices.
        g.
                addV("Person").property("name", "Lyndon").property("position", "Engineer").
                addV("Person").property("name", "Simon").property("position", "Engineer").
                addV("Person").property("name", "Grant").property("position", "Engineer").
                addV("Person").property("name", "Joe").property("position", "Manager").
                addV("Person").property("name", "Ishaan").property("position", "Product Manager").
                iterate();

        // Ensure that only these vertices are inserted without any connected data.
        List<Vertex> vertices = g.V().toList();
        Assert.assertEquals(5, vertices.size());
        for (final Vertex v : vertices) {
            // Verify that these vertices do not have data in their connections.
            Assert.assertNull(FireflyRecord.read(db, db.OUT_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.IN_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.IN_IN_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.IN_OUT_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.OUT_IN_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.OUT_OUT_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
        }

        // Let's create some edges.
        Vertex lyndon = g.V().has("name", "Lyndon").next();
        Vertex ishaan = g.V().has("name", "Ishaan").next();
        final Edge ishaanReferredLyndon = g.V().addE("referred").from(ishaan).to(lyndon).next();

        vertices = g.V().toList();
        Assert.assertEquals(5, vertices.size());
        for (final Vertex v : vertices) {
            // Validate that vertices that aren't Lyndon or Ishaan do not have data in them.
            if (v.id().equals(lyndon.id()) || v.id().equals(ishaan.id())) {
                continue;
            }
            Assert.assertNull(FireflyRecord.read(db, db.OUT_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.IN_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.IN_IN_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.IN_OUT_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.OUT_IN_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.OUT_OUT_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
        }

        VertexProperty<?> lyndonName = lyndon.property("name");
        VertexProperty<?> lyndonProfession = lyndon.property("position");
        VertexProperty<?> ishaanName = ishaan.property("name");
        VertexProperty<?> ishaanProfession = ishaan.property("position");

        // Validate that Lyndon and Ishaan have data in them.
        // For Ishaan we expect to have data in OUT_VP_SET and OUT_IN_SET, OUT_IN_SET will contain a list with an empty map.
        // For Lyndon we expect to have data in IN_VP_SET and IN_OUT_SET, IN_IN_SET will contain a list with an empty map.
        final FireflyRecord ishaanOutVPLyndon = FireflyRecord.read(db, db.OUT_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(ishaan.id())));
        final FireflyRecord lyndonInVPIshaan = FireflyRecord.read(db, db.IN_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(lyndon.id())));

        // Ishaan in.in and in.out and lyndon out.in and out.out are all null.
        Map<String, List<Map<String, List<Long>>>> ishaanInIn = null;
        Map<String, List<Map<String, List<Long>>>> ishaanInOut = null;
        Map<String, List<Map<String, List<Long>>>> lyndonOutIn = null;
        Map<String, List<Map<String, List<Long>>>> lyndonOutOut = null;

        // Ishaan out.in and lyndon in.out are maps with the referred edge at the tip.
        Map<String, List<Map<String, List<Long>>>> ishaanOutIn = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))))));
        Map<String, List<Map<String, List<Long>>>> lyndonInOut = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))))));

        // Ishaan out.out and lyndon in.in have initial edges but no compound edge so empty map at end.
        Map<String, List<Map<String, List<Long>>>> ishaanOutOut = Map.ofEntries(Map.entry("referred", List.of(new HashMap<>())));
        Map<String, List<Map<String, List<Long>>>> lyndonInIn = Map.ofEntries(Map.entry("referred", List.of(new HashMap<>())));

        //

        // Perform comparisons.
        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));
        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        // Ishaan out vp - ishaan goes out to Lyndon on referred.
        Map<String, List<Set<VertexProperty>>> ishaanOutVP = Map.ofEntries(Map.entry("referred", List.of(Set.of(lyndonName, lyndonProfession))));

        // Ishaan in vp - ishaan has no in edges - null.
        Map<String, List<Set<VertexProperty>>> ishaanInVP = null;

        final List<Object> ishaanReferredLyndonList = new ArrayList<>();
        ishaanReferredLyndonList.add(ishaanReferredLyndon.id());

        // Now add another edge and make sure that the compounding vertex properties are setup properly.
        Vertex simon = g.V().has("name", "Simon").next();
        final Edge lyndonReferredSimon = g.V().addE("referred").from(lyndon).to(simon).next();

        vertices = g.V().toList();
        Assert.assertEquals(5, vertices.size());
        for (final Vertex v : vertices) {
            // Validate that vertices that aren't Lyndon or Ishaan do not have data in them.
            if (v.id().equals(lyndon.id()) || v.id().equals(ishaan.id()) || v.id().equals(simon.id())) {
                continue;
            }
            Assert.assertNull(FireflyRecord.read(db, db.OUT_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.IN_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.IN_IN_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.IN_OUT_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.OUT_IN_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
            Assert.assertNull(FireflyRecord.read(db, db.OUT_OUT_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(v.id()))));
        }

        VertexProperty<?> simonName = simon.property("name");
        VertexProperty<?> simonProfession = simon.property("position");

        // Validate lyndon and Simon vertex property maps.
        // Simon out vp - simon has no out edges - null.
        Map<String, List<Set<VertexProperty>>> simonOutVp = null;

        // Simon in vp - simon goes in on referred to Lyndon.
        Map<String, List<Set<VertexProperty>>> simonInVp = Map.ofEntries(Map.entry("referred", List.of(Set.of(lyndonName, lyndonProfession))));

        // Lyndon in vp - Lyndon goes in on referred to Ishaan, in on manages to Joe, and in on worksWith to Grant.
        Map<String, List<Set<VertexProperty>>> lyndonInVp = Map.ofEntries(
                Map.entry("referred", List.of(Set.of(ishaanName, ishaanProfession))));

        // Lyndon out vp - Lyndon goes out on referred to Simon, out on managedBy to Joe, and out on worksWith to Grant.
        Map<String, List<Set<VertexProperty>>> lyndonOutVp = Map.ofEntries(
                Map.entry("referred", List.of(Set.of(simonName, simonProfession))));

        validateVertexProperty(ishaanOutVP, db.OUT_VP_SET, ishaan);
        validateVertexProperty(ishaanInVP, db.IN_VP_SET, ishaan);
        validateVertexProperty(simonOutVp, db.OUT_VP_SET, simon);
        validateVertexProperty(simonInVp, db.IN_VP_SET, simon);
        validateVertexProperty(lyndonInVp, db.IN_VP_SET, lyndon);
        validateVertexProperty(lyndonOutVp, db.OUT_VP_SET, lyndon);

        // Validate Simons empty sets are indeed empty.
        Assert.assertNull(FireflyRecord.read(db, db.OUT_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(simon.id()))));

        final List<Object> lyndonReferredSimonList = new ArrayList<>();
        lyndonReferredSimonList.add(lyndonReferredSimon.id());

        // Simon out.in and out.out maps are null.
        Map<String, List<Map<String, List<Long>>>> simonOutOut = null;
        Map<String, List<Map<String, List<Long>>>> simonOutIn = null;

        // Simon in.out goes from Lyndon back to Simon.
        Map<String, List<Map<String, List<Long>>>> simonInOut = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        // Simon in.in goes from Lyndon to Ishaan
        Map<String, List<Map<String, List<Long>>>> simonInIn = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))))));

        // Lyndon out.in goes from Simon to Lyndon.
        lyndonOutIn = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        // Lyndon out.out goes to Simon but has no out so is empty.
        lyndonOutOut = Map.ofEntries(Map.entry("referred", List.of(new HashMap<>())));

        // Ishaan now has out.out from lyndon to Simon
        ishaanOutOut = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        Assert.assertEquals(simonOutOut, getCompoundEdgeMap(simon, db.OUT_OUT_SET));
        Assert.assertEquals(simonOutIn, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(simonInIn, getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(simonInOut, getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));
        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        // Ishaan should now be updated with new bi-directional edge info.
        // Validate that the other records are still null.
        Assert.assertNull(FireflyRecord.read(db, db.IN_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(ishaan.id()))));

        // Currently we have Ishaan->Lyndon->Simon.
        // Now we can add some extra in/out edges from lyndon to see that the existing in and out vertex bi-directional edge maps are correct.
        final Vertex joe = g.V().has("name", "Joe").next();
        final Vertex grant = g.V().has("name", "Grant").next();
        ishaan = g.V().has("name", "Ishaan").next();
        simon = g.V().has("name", "Simon").next();
        lyndon = g.V().has("name", "Lyndon").next();

        Edge joeManagesLyndon = g.addE("manages").from(joe).to(lyndon).next();
        Edge lyndonManagedByJoe = g.addE("managedBy").from(lyndon).to(joe).next();
        Edge grantWorksWithLyndon = g.addE("worksWith").from(grant).to(lyndon).next();
        Edge lyndonWorksWithGrant = g.addE("worksWith").from(lyndon).to(grant).next();

        // Joe in.in - Joe goes in on managedBy to Lyndon who go in on worksWith to Grant, in on referred to Ishaan, and in on manages to Joe.
        Map<String, List<Map<String, List<Long>>>> joeInIn = Map.ofEntries(
                Map.entry("managedBy", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id()))),
                        Map.entry("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))))));

        // Joe in.out - Joe goes in on managedBy to Lyndon who goes out on worksWith to Grant, managedBy to Joe, and referred to Simon
        Map<String, List<Map<String, List<Long>>>> joeInOut = Map.ofEntries(
                Map.entry("managedBy", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))),
                        Map.entry("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        // Joe out.in - Joe goes out on manages to Lyndon who goes in on manages to Joe, worksWith to Grant, and referred to Ishaan
        Map<String, List<Map<String, List<Long>>>> joeOutIn = Map.ofEntries(
                Map.entry("manages", List.of(Map.ofEntries(
                        Map.entry("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id()))),
                        Map.entry("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))))));

        // Joe out.out - Joe goes out on manages to Lyndon who goes out on worksWith to Grant, managedBy to Joe, and referred to Simon
        Map<String, List<Map<String, List<Long>>>> joeOutOut = Map.ofEntries(
                Map.entry("manages", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))),
                        Map.entry("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        Assert.assertEquals(joeInIn, getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(joeInOut, getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(joeOutIn, getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(joeOutOut, getCompoundEdgeMap(joe, db.OUT_OUT_SET));

        // Grant in.in - Grant goes in on worksWith to Lyndon who goes in on worksWith to Grant, manages to Joe, and referred to Ishaan
        Map<String, List<Map<String, List<Long>>>> grantInIn = Map.ofEntries(
                Map.entry("worksWith", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id()))),
                        Map.entry("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))))));

        // Grant in.out - Grant goes in on worksWith to Lyndon who goes out on worksWith to Grant, managedBy to Joe, and referred to Simon
        Map<String, List<Map<String, List<Long>>>> grantInOut = Map.ofEntries(
                Map.entry("worksWith", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))),
                        Map.entry("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        // Grant out.in - Grant goes out on worksWith to Lyndon who goes in on worksWith to Grant, manages to Joe, and referred to Ishaan
        Map<String, List<Map<String, List<Long>>>> grantOutIn = Map.ofEntries(
                Map.entry("worksWith", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id()))),
                        Map.entry("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))))));

        // Grant out.out - Grant goes out on worksWith to Lyndon who goes out on worksWith to Grant, managedBy to Joe, and referred to Simon
        Map<String, List<Map<String, List<Long>>>> grantOutOut = Map.ofEntries(
                Map.entry("worksWith", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))),
                        Map.entry("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        Assert.assertEquals(grantInIn, getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(grantInOut, getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(grantOutIn, getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(grantOutOut, getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        // Simon in.out - Simon goes in on referred to Lyndon who goes out on worksWith to Grant, managedBy to Joe, and referred to Simon
        simonInOut = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))),
                        Map.entry("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        // Simon in.in - Simon goes in on referred to Lyndon who goes in on worksWith to Grant, manages to Joe, and referred to Ishaan
        simonInIn = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id()))),
                        Map.entry("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))))));

        // Simon out.in and out.out - Simon has no out edges - null. (set previously)
        Assert.assertEquals(simonInIn, getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(simonInOut, getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(simonOutIn, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(simonOutOut, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        // Ishaan in.out and in.in - Ishaan has no in edges - null. (set previously)

        // Ishaan out.in - Ishaan goes in on referred to Lyndon who goes in on worksWith to Grant, manages to Joe, and referred to Ishaan
        ishaanOutIn = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id()))),
                        Map.entry("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))))));

        // Ishaan out.out - Ishaan goes out on referred to Lyndon who goes out on worksWith to Grant, managedBy to Joe, and referred to Simon
        ishaanOutOut = Map.ofEntries(
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))),
                        Map.entry("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id()))),
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        // Lyndon in.in - Lyndon goes in on manages to Joe, worksWith to Grant, and referred to Ishaan.
        //  On manages to Joe, Joes in edge managedBy goes back to Lyndon.
        //  On worksWith to Grant, Grants in edge worksWith goes back to Lyndon.
        //  On referred to Ishaan, there are no in edges.
        lyndonInIn = Map.ofEntries(
                Map.entry("manages", List.of(Map.ofEntries(
                        Map.entry("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))))),
                Map.entry("worksWith", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))))),
                Map.entry("referred", List.of(new HashMap<>())));

        // Lyndon in.out - Lyndon goes in on manages to Joe, worksWith to Grant, and referred to Ishaan.
        //  On manages to Joe, Joes out edge manages goes to Lyndon.
        //  On worksWith to Grant, Grants out edge worksWith goes to Lyndon.
        //  On referred to Ishaan, Ishaans out edge referred goes to Lyndon.
        lyndonInOut = Map.ofEntries(
                Map.entry("manages", List.of(Map.ofEntries(
                        Map.entry("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))))),
                Map.entry("worksWith", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))))),
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))))));

        // Lyndon out.out - Lyndon goes out on managedBy to Joe, worksWith to Grant, and referred to Simon.
        //  On managedBy to Joe, Joes out edge manages goes to Lyndon.
        //  On worksWith to Grant, Grants out edge worksWith goes to Lyndon.
        //  On referred to Simon, there are no out edges.
        lyndonOutOut = Map.ofEntries(
                Map.entry("managedBy", List.of(Map.ofEntries(
                        Map.entry("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))))),
                Map.entry("worksWith", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))))),
                Map.entry("referred", List.of(new HashMap<>())));

        // Lyndon out.in - Lyndon goes out on managedBy to Joe, worksWith to Grant, and referred to Simon.
        //  On managedBy to Joe, Joes in edge managedBy goes back to Lyndon.
        //  On worksWith to Grant, Grants in edge worksWith goes back to Lyndon.
        //  On referred to Simon, Simons in edge referred goes to Lyndon.
        lyndonOutIn = Map.ofEntries(
                Map.entry("managedBy", List.of(Map.ofEntries(
                        Map.entry("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))))),
                Map.entry("worksWith", List.of(Map.ofEntries(
                        Map.entry("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))))),
                Map.entry("referred", List.of(Map.ofEntries(
                        Map.entry("referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))))));

        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        //
        // Validate vertex properties.
        //
        VertexProperty<?> grantName = grant.property("name");
        VertexProperty<?> grantProfession = grant.property("position");
        VertexProperty<?> joeName = joe.property("name");
        VertexProperty<?> joeProfession = joe.property("position");

        // Grant out vp - grant goes out on worksWith to Lyndon.
        Map<String, List<Set<VertexProperty>>> grantOutVp = Map.ofEntries(Map.entry("worksWith", List.of(Set.of(lyndonName, lyndonProfession))));

        // Grant in vp - grant goes in on worksWith to Lyndon.
        Map<String, List<Set<VertexProperty>>> grantInVp = Map.ofEntries(Map.entry("worksWith", List.of(Set.of(lyndonName, lyndonProfession))));

        // Joe out vp - joe goes out on manages to Lyndon.
        Map<String, List<Set<VertexProperty>>> joeOutVp = Map.ofEntries(Map.entry("manages", List.of(Set.of(lyndonName, lyndonProfession))));

        // Joe in vp - joe goes in on managedBy to Lyndon.
        Map<String, List<Set<VertexProperty>>> joeInVp = Map.ofEntries(Map.entry("managedBy", List.of(Set.of(lyndonName, lyndonProfession))));

        // Lyndon in vp - Lyndon goes in on referred to Ishaan, in on manages to Joe, and in on worksWith to Grant.
        lyndonInVp = Map.ofEntries(
                Map.entry("referred", List.of(Set.of(ishaanName, ishaanProfession))),
                Map.entry("manages", List.of(Set.of(joeName, joeProfession))),
                Map.entry("worksWith", List.of(Set.of(grantName, grantProfession))));

        // Lyndon out vp - Lyndon goes out on referred to Simon, out on managedBy to Joe, and out on worksWith to Grant.
        lyndonOutVp = Map.ofEntries(
                Map.entry("referred", List.of(Set.of(simonName, simonProfession))),
                Map.entry("managedBy", List.of(Set.of(joeName, joeProfession))),
                Map.entry("worksWith", List.of(Set.of(grantName, grantProfession))));

        validateVertexProperty(grantOutVp, db.OUT_VP_SET, grant);
        validateVertexProperty(grantInVp, db.IN_VP_SET, grant);
        validateVertexProperty(joeOutVp, db.OUT_VP_SET, joe);
        validateVertexProperty(joeInVp, db.IN_VP_SET, joe);
        validateVertexProperty(ishaanOutVP, db.OUT_VP_SET, ishaan);
        validateVertexProperty(ishaanInVP, db.IN_VP_SET, ishaan);
        validateVertexProperty(simonOutVp, db.OUT_VP_SET, simon);
        validateVertexProperty(simonInVp, db.IN_VP_SET, simon);
        validateVertexProperty(lyndonInVp, db.IN_VP_SET, lyndon);
        validateVertexProperty(lyndonOutVp, db.OUT_VP_SET, lyndon);
    }

    Map<String, List<Map<String, List<Long>>>> getCompoundEdgeMap(final Vertex vertex, final String set) {
        FireflyRecord ffr = FireflyRecord.read(db, set, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(vertex.id())));
        if (ffr == null) {
            return null;
        } else {
            return (Map<String, List<Map<String, List<Long>>>>) ffr.record.getMap(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN);
        }
    }

    void validateVertexProperty(final Map<String, List<Set<VertexProperty>>> expectedProperties,
                                final String set,
                                final Vertex vertex) {
        FireflyRecord ffr = FireflyRecord.read(db, set, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(vertex.id())));
        if (ffr != null) {
            final Map<String, List<Map<String, Long>>> vpIdMap = (Map<String, List<Map<String, Long>>>) ffr.record.getMap(db.VERTEX_PROPERTY_NAME_TO_ID);
            final Map<String, List<Map<String, Object>>> vpValueMap = (Map<String, List<Map<String, Object>>>) ffr.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE);
            final Map<String, List<Map<String, Long>>> vpTypeHintMap = (Map<String, List<Map<String, Long>>>) ffr.record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT);
            final Map<String, List<Map<String, Long>>> vpIdMapExpected = new HashMap<>();
            final Map<String, List<Map<String, Object>>> vpValueMapExpected = new HashMap<>();
            final Map<String, List<Map<String, Long>>> vpTypeHintMapExpected = new HashMap<>();

            expectedProperties.forEach((key, value) -> {
                final List<Map<String, Long>> vpIdList = new ArrayList<>();
                final List<Map<String, Object>> vpValueList = new ArrayList<>();
                final List<Map<String, Long>> vpTypeHintList = new ArrayList<>();
                value.forEach(setOfVPs -> {
                    final Map<String, Long> vpIdMapInner = new HashMap<>();
                    final Map<String, Object> vpValueMapInner = new HashMap<>();
                    final Map<String, Long> vpTypeHintMapInner = new HashMap<>();
                    setOfVPs.forEach(vp -> {
                        vpIdMapInner.put(vp.key(), NumericIdManager.convert(vp.id()));
                        vpValueMapInner.put(vp.key(), vp.value());
                        vpTypeHintMapInner.put(vp.key(), db.getSupportedType(vp.value().getClass()));
                    });
                    vpIdList.add(vpIdMapInner);
                    vpValueList.add(vpValueMapInner);
                    vpTypeHintList.add(vpTypeHintMapInner);
                });
                vpIdMapExpected.put(key, vpIdList);
                vpValueMapExpected.put(key, vpValueList);
                vpTypeHintMapExpected.put(key, vpTypeHintList);
            });
            Assert.assertEquals(vpIdMapExpected, vpIdMap);
            Assert.assertEquals(vpValueMapExpected, vpValueMap);
            Assert.assertEquals(vpTypeHintMapExpected, vpTypeHintMap);
        } else {
            Assert.assertNull(expectedProperties);
        }
    }


    @Test
    public void testStarPackedGraphRemoveThreePerson() {
        // Want to ensure removal leaves no artifacts / there is no issues leaving empty items as opposed to null items.
        // To do this, we can call the functions a bunch #science.
        testRemoval();
        testRemoval();
        testDropRemoval();
        testRemoval();
        testDropRemoval();
        testDropRemoval();
        testDropRemoval();
    }

    @Test
    public void testAddAndRemoveVertexProperty() {
        final GraphTraversalSource g = graph.traversal();
        final AerospikeConnection db = graph.getBaseGraph();

        // Insert 5 vertices.
        g.
                addV("Person").property("name", "Lyndon").property("position", "Engineer").
                addV("Person").property("name", "Simon").property("position", "Engineer").
                addV("Person").property("name", "Grant").property("position", "Engineer").
                addV("Person").property("name", "Joe").property("position", "Manager").
                addV("Person").property("name", "Ishaan").property("position", "Product Manager").
                iterate();


        Vertex lyndon = g.V().has("name", "Lyndon").next();
        Vertex ishaan = g.V().has("name", "Ishaan").next();
        Vertex simon = g.V().has("name", "Simon").next();
        Vertex joe = g.V().has("name", "Joe").next();
        Vertex grant = g.V().has("name", "Grant").next();
        final VertexProperty lyndonName = lyndon.property("name");
        final VertexProperty lyndonPosition = lyndon.property("position");
        final VertexProperty ishaanName = ishaan.property("name");
        final VertexProperty ishaanPosition = ishaan.property("position");
        final VertexProperty simonName = simon.property("name");
        final VertexProperty simonPosition = simon.property("position");
        final VertexProperty joeName = joe.property("name");
        final VertexProperty joePosition = joe.property("position");
        final VertexProperty grantName = grant.property("name");
        final VertexProperty grantPosition = grant.property("position");
        final Edge ishaanReferredLyndon = g.V().addE("referred").from(ishaan).to(lyndon).next();
        final Edge lyndonReferredSimon = g.V().addE("referred").from(lyndon).to(simon).next();
        final Edge joeManagesLyndon = g.addE("manages").from(joe).to(lyndon).next();
        final Edge lyndonManagedByJoe = g.addE("managedBy").from(lyndon).to(joe).next();
        final Edge grantWorksWithLyndon = g.addE("worksWith").from(grant).to(lyndon).next();
        final Edge lyndonWorksWithGrant = g.addE("worksWith").from(lyndon).to(grant).next();

        Map<String, List<Set<VertexProperty>>> lyndonOutVP = Map.of(
                "referred", List.of(Set.of(simonName, simonPosition)),
                "managedBy", List.of(Set.of(joeName, joePosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition))
        );
        Map<String, List<Set<VertexProperty>>> lyndonInVP = Map.of(
                "referred", List.of(Set.of(ishaanName, ishaanPosition)),
                "manages", List.of(Set.of(joeName, joePosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition))
        );
        Map<String, List<Map<String, List<Long>>>> lyndonOutOut = Map.of(
                "referred", List.of(Map.of()),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))),
                "managedBy", List.of(Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))));
        Map<String, List<Map<String, List<Long>>>> lyndonInIn = Map.of(
                "referred", List.of(Map.of()),
                "manages", List.of(Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))));
        Map<String, List<Map<String, List<Long>>>> lyndonInOut = Map.of(
                "referred", List.of(Map.of("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))),
                "manages", List.of(Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))));
        Map<String, List<Map<String, List<Long>>>> lyndonOutIn = Map.of(
                "managedBy", List.of(Map.of(
                        "managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))),
                "referred", List.of(Map.of(
                        "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))));

        Map<String, List<Set<VertexProperty>>> ishaanOutVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Set<VertexProperty>>> ishaanInVP = null;
        Map<String, List<Map<String, List<Long>>>> ishaanOutOut = Map.of("referred", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> ishaanInIn = null;
        Map<String, List<Map<String, List<Long>>>> ishaanInOut = null;
        Map<String, List<Map<String, List<Long>>>> ishaanOutIn = Map.of(
                "referred", List.of(
                        Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                               "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                               "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
                ));

        Map<String, List<Set<VertexProperty>>> simonOutVP = null;
        Map<String, List<Set<VertexProperty>>> simonInVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Map<String, List<Long>>>> simonInIn = Map.of("referred", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> simonOutOut = null;
        Map<String, List<Map<String, List<Long>>>> simonInOut = Map.of("referred", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> simonOutIn = null;

        Map<String, List<Set<VertexProperty>>> grantOutVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Set<VertexProperty>>> grantInVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Map<String, List<Long>>>> grantInIn = Map.of("worksWith", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> grantOutOut = Map.of("worksWith", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> grantInOut = Map.of("worksWith", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> grantOutIn = Map.of("worksWith", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));

        Map<String, List<Set<VertexProperty>>> joeOutVP = Map.of("manages", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Set<VertexProperty>>> joeInVP = Map.of("managedBy", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Map<String, List<Long>>>> joeInIn = Map.of("managedBy", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> joeOutOut = Map.of("manages", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> joeInOut = Map.of("managedBy", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> joeOutIn = Map.of("manages", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));

        validateVertexProperty(lyndonOutVP, db.OUT_VP_SET, lyndon);
        validateVertexProperty(lyndonInVP, db.IN_VP_SET, lyndon);
        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(ishaanOutVP, db.OUT_VP_SET, ishaan);
        validateVertexProperty(ishaanInVP, db.IN_VP_SET, ishaan);
        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(simonOutVP, db.OUT_VP_SET, simon);
        validateVertexProperty(simonInVP, db.IN_VP_SET, simon);
        Assert.assertEquals(simonInIn, getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(simonInOut, getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(simonOutIn, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(simonOutOut, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(grantOutVP, db.OUT_VP_SET, grant);
        validateVertexProperty(grantInVP, db.IN_VP_SET, grant);
        Assert.assertEquals(grantInIn, getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(grantInOut, getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(grantOutIn, getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(grantOutOut, getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(joeOutVP, db.OUT_VP_SET, joe);
        validateVertexProperty(joeInVP, db.IN_VP_SET, joe);
        Assert.assertEquals(joeInIn, getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(joeInOut, getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(joeOutIn, getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(joeOutOut, getCompoundEdgeMap(joe, db.OUT_OUT_SET));

        g.V().has("name", "Lyndon").property("age", 29L).iterate();
        lyndon = g.V().has("name", "Lyndon").next();
        final VertexProperty lyndonAge = lyndon.property("age");
        g.V().has("name", "Simon").property("age", "old").iterate();
        simon = g.V().has("name", "Simon").next();
        final VertexProperty simonAge = simon.property("age");

        lyndonOutVP = Map.of(
                "referred", List.of(Set.of(simonName, simonPosition, simonAge)),
                "managedBy", List.of(Set.of(joeName, joePosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition))
        );
        ishaanOutVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition, lyndonAge)));
        simonInVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition, lyndonAge)));
        grantOutVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition, lyndonAge)));
        grantInVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition, lyndonAge)));
        joeOutVP = Map.of("manages", List.of(Set.of(lyndonName, lyndonPosition, lyndonAge)));
        joeInVP = Map.of("managedBy", List.of(Set.of(lyndonName, lyndonPosition, lyndonAge)));

        dumpInfo(lyndon);
        dumpInfo(ishaan);
        dumpInfo(simon);
        validateVertexProperty(lyndonOutVP, db.OUT_VP_SET, lyndon);
        validateVertexProperty(lyndonInVP, db.IN_VP_SET, lyndon);
        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(ishaanOutVP, db.OUT_VP_SET, ishaan);
        validateVertexProperty(ishaanInVP, db.IN_VP_SET, ishaan);
        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(simonOutVP, db.OUT_VP_SET, simon);
        validateVertexProperty(simonInVP, db.IN_VP_SET, simon);
        Assert.assertEquals(simonInIn, getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(simonInOut, getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(simonOutIn, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(simonOutOut, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(grantOutVP, db.OUT_VP_SET, grant);
        validateVertexProperty(grantInVP, db.IN_VP_SET, grant);
        Assert.assertEquals(grantInIn, getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(grantInOut, getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(grantOutIn, getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(grantOutOut, getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(joeOutVP, db.OUT_VP_SET, joe);
        validateVertexProperty(joeInVP, db.IN_VP_SET, joe);
        Assert.assertEquals(joeInIn, getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(joeInOut, getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(joeOutIn, getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(joeOutOut, getCompoundEdgeMap(joe, db.OUT_OUT_SET));

        simonAge.remove();
        lyndonAge.remove();

        lyndonOutVP = Map.of(
                "referred", List.of(Set.of(simonName, simonPosition)),
                "managedBy", List.of(Set.of(joeName, joePosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition))
        );
        ishaanOutVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition)));
        simonInVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition)));
        grantOutVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition)));
        grantInVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition)));
        joeOutVP = Map.of("manages", List.of(Set.of(lyndonName, lyndonPosition)));
        joeInVP = Map.of("managedBy", List.of(Set.of(lyndonName, lyndonPosition)));

        validateVertexProperty(lyndonOutVP, db.OUT_VP_SET, lyndon);
        validateVertexProperty(lyndonInVP, db.IN_VP_SET, lyndon);
        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(ishaanOutVP, db.OUT_VP_SET, ishaan);
        validateVertexProperty(ishaanInVP, db.IN_VP_SET, ishaan);
        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(simonOutVP, db.OUT_VP_SET, simon);
        validateVertexProperty(simonInVP, db.IN_VP_SET, simon);
        Assert.assertEquals(simonInIn, getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(simonInOut, getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(simonOutIn, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(simonOutOut, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(grantOutVP, db.OUT_VP_SET, grant);
        validateVertexProperty(grantInVP, db.IN_VP_SET, grant);
        Assert.assertEquals(grantInIn, getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(grantInOut, getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(grantOutIn, getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(grantOutOut, getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(joeOutVP, db.OUT_VP_SET, joe);
        validateVertexProperty(joeInVP, db.IN_VP_SET, joe);
        Assert.assertEquals(joeInIn, getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(joeInOut, getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(joeOutIn, getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(joeOutOut, getCompoundEdgeMap(joe, db.OUT_OUT_SET));
    }

    void testRemoval() {
        final GraphTraversalSource g = graph.traversal();
        final AerospikeConnection db = graph.getBaseGraph();

        // Insert 5 vertices.
        g.
                addV("Person").property("name", "Lyndon").property("position", "Engineer").
                addV("Person").property("name", "Simon").property("position", "Engineer").
                addV("Person").property("name", "Grant").property("position", "Engineer").
                addV("Person").property("name", "Joe").property("position", "Manager").
                addV("Person").property("name", "Ishaan").property("position", "Product Manager").
                iterate();


        final Vertex lyndon = g.V().has("name", "Lyndon").next();
        final Vertex ishaan = g.V().has("name", "Ishaan").next();
        final Vertex simon = g.V().has("name", "Simon").next();
        final Vertex joe = g.V().has("name", "Joe").next();
        final Vertex grant = g.V().has("name", "Grant").next();
        final VertexProperty lyndonName = lyndon.property("name");
        final VertexProperty lyndonPosition = lyndon.property("position");
        final VertexProperty ishaanName = ishaan.property("name");
        final VertexProperty ishaanPosition = ishaan.property("position");
        final VertexProperty simonName = simon.property("name");
        final VertexProperty simonPosition = simon.property("position");
        final VertexProperty joeName = joe.property("name");
        final VertexProperty joePosition = joe.property("position");
        final VertexProperty grantName = grant.property("name");
        final VertexProperty grantPosition = grant.property("position");
        final Edge ishaanReferredLyndon = g.V().addE("referred").from(ishaan).to(lyndon).next();
        final Edge lyndonReferredSimon = g.V().addE("referred").from(lyndon).to(simon).next();
        final Edge joeManagesLyndon = g.addE("manages").from(joe).to(lyndon).next();
        final Edge lyndonManagedByJoe = g.addE("managedBy").from(lyndon).to(joe).next();
        final Edge grantWorksWithLyndon = g.addE("worksWith").from(grant).to(lyndon).next();
        final Edge lyndonWorksWithGrant = g.addE("worksWith").from(lyndon).to(grant).next();

        Map<String, List<Set<VertexProperty>>> lyndonOutVP = Map.of(
                "referred", List.of(Set.of(simonName, simonPosition)),
                "managedBy", List.of(Set.of(joeName, joePosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition))
        );
        Map<String, List<Set<VertexProperty>>> lyndonInVP = Map.of(
                "referred", List.of(Set.of(ishaanName, ishaanPosition)),
                "manages", List.of(Set.of(joeName, joePosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition))
        );
        Map<String, List<Map<String, List<Long>>>> lyndonOutOut = Map.of(
                "referred", List.of(Map.of()),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))),
                "managedBy", List.of(Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))));
        Map<String, List<Map<String, List<Long>>>> lyndonInIn = Map.of(
                "referred", List.of(Map.of()),
                "manages", List.of(Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))));
        Map<String, List<Map<String, List<Long>>>> lyndonInOut = Map.of(
                "referred", List.of(Map.of("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))),
                "manages", List.of(Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))));
        Map<String, List<Map<String, List<Long>>>> lyndonOutIn = Map.of(
                "managedBy", List.of(Map.of(
                        "managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))),
                "referred", List.of(Map.of(
                        "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))));

        Map<String, List<Set<VertexProperty>>> ishaanOutVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Set<VertexProperty>>> ishaanInVP = null;
        Map<String, List<Map<String, List<Long>>>> ishaanOutOut = Map.of("referred", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> ishaanInIn = null;
        Map<String, List<Map<String, List<Long>>>> ishaanInOut = null;
        Map<String, List<Map<String, List<Long>>>> ishaanOutIn = Map.of(
                "referred", List.of(
                        Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                               "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                               "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
                ));

        Map<String, List<Set<VertexProperty>>> simonOutVP = null;
        Map<String, List<Set<VertexProperty>>> simonInVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Map<String, List<Long>>>> simonInIn = Map.of("referred", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> simonOutOut = null;
        Map<String, List<Map<String, List<Long>>>> simonInOut = Map.of("referred", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> simonOutIn = null;

        Map<String, List<Set<VertexProperty>>> grantOutVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Set<VertexProperty>>> grantInVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Map<String, List<Long>>>> grantInIn = Map.of("worksWith", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> grantOutOut = Map.of("worksWith", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> grantInOut = Map.of("worksWith", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> grantOutIn = Map.of("worksWith", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));

        Map<String, List<Set<VertexProperty>>> joeOutVP = Map.of("manages", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Set<VertexProperty>>> joeInVP = Map.of("managedBy", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Map<String, List<Long>>>> joeInIn = Map.of("managedBy", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> joeOutOut = Map.of("manages", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> joeInOut = Map.of("managedBy", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> joeOutIn = Map.of("manages", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));

        validateVertexProperty(lyndonOutVP, db.OUT_VP_SET, lyndon);
        validateVertexProperty(lyndonInVP, db.IN_VP_SET, lyndon);
        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(ishaanOutVP, db.OUT_VP_SET, ishaan);
        validateVertexProperty(ishaanInVP, db.IN_VP_SET, ishaan);
        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(simonOutVP, db.OUT_VP_SET, simon);
        validateVertexProperty(simonInVP, db.IN_VP_SET, simon);
        Assert.assertEquals(simonInIn, getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(simonInOut, getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(simonOutIn, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(simonOutOut, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(grantOutVP, db.OUT_VP_SET, grant);
        validateVertexProperty(grantInVP, db.IN_VP_SET, grant);
        Assert.assertEquals(grantInIn, getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(grantInOut, getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(grantOutIn, getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(grantOutOut, getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(joeOutVP, db.OUT_VP_SET, joe);
        validateVertexProperty(joeInVP, db.IN_VP_SET, joe);
        Assert.assertEquals(joeInIn, getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(joeInOut, getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(joeOutIn, getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(joeOutOut, getCompoundEdgeMap(joe, db.OUT_OUT_SET));

        // Lets remove lyndon and simons link and make sure it is removed from the compound edge map and vertex properties chaining.
        lyndonReferredSimon.remove();

        lyndonOutVP = Map.of(
                "managedBy", List.of(Set.of(joeName, joePosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition)));
        lyndonOutOut = Map.of(
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))),
                "managedBy", List.of(Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))));
        lyndonOutIn = Map.of(
                "managedBy", List.of(Map.of(
                        "managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))));

        ishaanOutOut = Map.of("referred", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))));

        simonInVP = new HashMap<>();
        simonInIn = new HashMap<>();
        simonInOut = new HashMap<>();

        grantOutOut = Map.of("worksWith", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))
                )));
        grantInOut = Map.of("worksWith", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))
                )));

        joeOutOut = Map.of("manages", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))
                )));
        joeInOut = Map.of("managedBy", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id()))
                )));

        validateVertexProperty(lyndonOutVP, db.OUT_VP_SET, lyndon);
        validateVertexProperty(lyndonInVP, db.IN_VP_SET, lyndon);
        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(ishaanOutVP, db.OUT_VP_SET, ishaan);
        validateVertexProperty(ishaanInVP, db.IN_VP_SET, ishaan);
        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(simonOutVP, db.OUT_VP_SET, simon);
        validateVertexProperty(simonInVP, db.IN_VP_SET, simon);
        Assert.assertEquals(simonInIn, getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(simonInOut, getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(simonOutIn, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(simonOutOut, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(grantOutVP, db.OUT_VP_SET, grant);
        validateVertexProperty(grantInVP, db.IN_VP_SET, grant);
        Assert.assertEquals(grantInIn, getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(grantInOut, getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(grantOutIn, getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(grantOutOut, getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(joeOutVP, db.OUT_VP_SET, joe);
        validateVertexProperty(joeInVP, db.IN_VP_SET, joe);
        Assert.assertEquals(joeInIn, getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(joeInOut, getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(joeOutIn, getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(joeOutOut, getCompoundEdgeMap(joe, db.OUT_OUT_SET));

        // Remove one link from joe and lyndon
        joeManagesLyndon.remove();

        lyndonInVP = Map.of(
                "referred", List.of(Set.of(ishaanName, ishaanPosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition))
        );
        lyndonOutOut = Map.of(
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))),
                "managedBy", List.of(Map.of()));
        lyndonInIn = Map.of(
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))),
                "referred", List.of(Map.of()));
        lyndonInOut = Map.of(
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))),
                "referred", List.of(Map.of("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))));
        lyndonOutIn = Map.of(
                "managedBy", List.of(Map.of(
                        "managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))));

        ishaanOutIn = Map.of(
                "referred", List.of(Map.of("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())),
                                           "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))));

        grantInIn = Map.of("worksWith", List.of(
                Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        grantOutIn = Map.of("worksWith", List.of(
                Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));

        joeOutVP = new HashMap<>();
        joeInIn = Map.of("managedBy", List.of(
                Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        joeOutOut = new HashMap<>();
        joeOutIn = new HashMap<>();

        validateVertexProperty(lyndonOutVP, db.OUT_VP_SET, lyndon);
        validateVertexProperty(lyndonInVP, db.IN_VP_SET, lyndon);
        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(ishaanOutVP, db.OUT_VP_SET, ishaan);
        validateVertexProperty(ishaanInVP, db.IN_VP_SET, ishaan);
        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(simonOutVP, db.OUT_VP_SET, simon);
        validateVertexProperty(simonInVP, db.IN_VP_SET, simon);
        Assert.assertEquals(simonInIn, getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(simonInOut, getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(simonOutIn, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(simonOutOut, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(grantOutVP, db.OUT_VP_SET, grant);
        validateVertexProperty(grantInVP, db.IN_VP_SET, grant);
        Assert.assertEquals(grantInIn, getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(grantInOut, getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(grantOutIn, getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(grantOutOut, getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(joeOutVP, db.OUT_VP_SET, joe);
        validateVertexProperty(joeInVP, db.IN_VP_SET, joe);
        Assert.assertEquals(joeInIn, getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(joeInOut, getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(joeOutIn, getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(joeOutOut, getCompoundEdgeMap(joe, db.OUT_OUT_SET));

        // Let's remove lyndon's out edges now. lyndonWorksWithGrant and lyndonManagedByJoe.
        lyndonWorksWithGrant.remove();
        lyndonManagedByJoe.remove();

        lyndonOutVP = new HashMap<>();
        lyndonOutOut = Map.of();
        lyndonInIn = Map.of("worksWith", List.of(Map.of()),
                            "referred", List.of(Map.of()));
        lyndonInOut = Map.of(
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))),
                "referred", List.of(Map.of("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))));
        lyndonOutIn = Map.of();

        ishaanOutOut = Map.of("referred", List.of(Map.of()));

        grantInVP = new HashMap<>();
        grantInIn = Map.of();
        grantInOut = Map.of();
        grantOutOut = Map.of("worksWith", List.of(Map.of()));

        joeInVP = new HashMap<>();
        joeInIn = new HashMap<>();
        joeInOut = Map.of();

        validateVertexProperty(lyndonOutVP, db.OUT_VP_SET, lyndon);
        validateVertexProperty(lyndonInVP, db.IN_VP_SET, lyndon);
        Assert.assertEquals(lyndonInIn, getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(lyndonInOut, getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(lyndonOutIn, getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(lyndonOutOut, getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(ishaanOutVP, db.OUT_VP_SET, ishaan);
        validateVertexProperty(ishaanInVP, db.IN_VP_SET, ishaan);
        Assert.assertEquals(ishaanInIn, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(ishaanInOut, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(ishaanOutIn, getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(ishaanOutOut, getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(simonOutVP, db.OUT_VP_SET, simon);
        validateVertexProperty(simonInVP, db.IN_VP_SET, simon);
        Assert.assertEquals(simonInIn, getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(simonInOut, getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(simonOutIn, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(simonOutOut, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(grantOutVP, db.OUT_VP_SET, grant);
        validateVertexProperty(grantInVP, db.IN_VP_SET, grant);
        Assert.assertEquals(grantInIn, getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(grantInOut, getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(grantOutIn, getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(grantOutOut, getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(joeOutVP, db.OUT_VP_SET, joe);
        validateVertexProperty(joeInVP, db.IN_VP_SET, joe);
        Assert.assertEquals(joeInIn, getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(joeInOut, getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(joeOutIn, getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(joeOutOut, getCompoundEdgeMap(joe, db.OUT_OUT_SET));

        // Remove final edges.
        grantWorksWithLyndon.remove();
        ishaanReferredLyndon.remove();

        validateVertexProperty(Map.of(), db.OUT_VP_SET, lyndon);
        validateVertexProperty(Map.of(), db.IN_VP_SET, lyndon);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(Map.of(), db.OUT_VP_SET, ishaan);
        validateVertexProperty(null, db.IN_VP_SET, ishaan);
        Assert.assertEquals(null, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(null, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(null, db.OUT_VP_SET, simon);
        validateVertexProperty(Map.of(), db.IN_VP_SET, simon);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(null, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(null, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(Map.of(), db.OUT_VP_SET, grant);
        validateVertexProperty(Map.of(), db.IN_VP_SET, grant);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(Map.of(), db.OUT_VP_SET, joe);
        validateVertexProperty(Map.of(), db.IN_VP_SET, joe);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.OUT_OUT_SET));

        g.V().drop().iterate();

        validateVertexProperty(Map.of(), db.OUT_VP_SET, lyndon);
        validateVertexProperty(Map.of(), db.IN_VP_SET, lyndon);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(Map.of(), db.OUT_VP_SET, ishaan);
        validateVertexProperty(null, db.IN_VP_SET, ishaan);
        Assert.assertEquals(null, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(null, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(null, db.OUT_VP_SET, simon);
        validateVertexProperty(Map.of(), db.IN_VP_SET, simon);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(null, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(null, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(Map.of(), db.OUT_VP_SET, grant);
        validateVertexProperty(Map.of(), db.IN_VP_SET, grant);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(Map.of(), db.OUT_VP_SET, joe);
        validateVertexProperty(Map.of(), db.IN_VP_SET, joe);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.OUT_OUT_SET));
    }

    void testDropRemoval() {
        final GraphTraversalSource g = graph.traversal();
        final AerospikeConnection db = graph.getBaseGraph();

        // Insert 5 vertices.
        g.
                addV("Person").property("name", "Lyndon").property("position", "Engineer").
                addV("Person").property("name", "Simon").property("position", "Engineer").
                addV("Person").property("name", "Grant").property("position", "Engineer").
                addV("Person").property("name", "Joe").property("position", "Manager").
                addV("Person").property("name", "Ishaan").property("position", "Product Manager").
                iterate();


        final Vertex lyndon = g.V().has("name", "Lyndon").next();
        final Vertex ishaan = g.V().has("name", "Ishaan").next();
        final Vertex simon = g.V().has("name", "Simon").next();
        final Vertex joe = g.V().has("name", "Joe").next();
        final Vertex grant = g.V().has("name", "Grant").next();
        final VertexProperty lyndonName = lyndon.property("name");
        final VertexProperty lyndonPosition = lyndon.property("position");
        final VertexProperty ishaanName = ishaan.property("name");
        final VertexProperty ishaanPosition = ishaan.property("position");
        final VertexProperty simonName = simon.property("name");
        final VertexProperty simonPosition = simon.property("position");
        final VertexProperty joeName = joe.property("name");
        final VertexProperty joePosition = joe.property("position");
        final VertexProperty grantName = grant.property("name");
        final VertexProperty grantPosition = grant.property("position");
        final Edge ishaanReferredLyndon = g.V().addE("referred").from(ishaan).to(lyndon).next();
        final Edge lyndonReferredSimon = g.V().addE("referred").from(lyndon).to(simon).next();
        final Edge joeManagesLyndon = g.addE("manages").from(joe).to(lyndon).next();
        final Edge lyndonManagedByJoe = g.addE("managedBy").from(lyndon).to(joe).next();
        final Edge grantWorksWithLyndon = g.addE("worksWith").from(grant).to(lyndon).next();
        final Edge lyndonWorksWithGrant = g.addE("worksWith").from(lyndon).to(grant).next();

        Map<String, List<Set<VertexProperty>>> lyndonOutVP = Map.of(
                "referred", List.of(Set.of(simonName, simonPosition)),
                "managedBy", List.of(Set.of(joeName, joePosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition))
        );
        Map<String, List<Set<VertexProperty>>> lyndonInVP = Map.of(
                "referred", List.of(Set.of(ishaanName, ishaanPosition)),
                "manages", List.of(Set.of(joeName, joePosition)),
                "worksWith", List.of(Set.of(grantName, grantPosition))
        );
        Map<String, List<Map<String, List<Long>>>> lyndonOutOut = Map.of(
                "referred", List.of(Map.of()),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))),
                "managedBy", List.of(Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))));
        Map<String, List<Map<String, List<Long>>>> lyndonInIn = Map.of(
                "referred", List.of(Map.of()),
                "manages", List.of(Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))));
        Map<String, List<Map<String, List<Long>>>> lyndonInOut = Map.of(
                "referred", List.of(Map.of("referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))),
                "manages", List.of(Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())))));
        Map<String, List<Map<String, List<Long>>>> lyndonOutIn = Map.of(
                "managedBy", List.of(Map.of(
                        "managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())))),
                "worksWith", List.of(Map.of("worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())))),
                "referred", List.of(Map.of(
                        "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))));

        Map<String, List<Set<VertexProperty>>> ishaanOutVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Set<VertexProperty>>> ishaanInVP = null;
        Map<String, List<Map<String, List<Long>>>> ishaanOutOut = Map.of("referred", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> ishaanInIn = null;
        Map<String, List<Map<String, List<Long>>>> ishaanInOut = null;
        Map<String, List<Map<String, List<Long>>>> ishaanOutIn = Map.of(
                "referred", List.of(
                        Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                               "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                               "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
                ));

        Map<String, List<Set<VertexProperty>>> simonOutVP = null;
        Map<String, List<Set<VertexProperty>>> simonInVP = Map.of("referred", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Map<String, List<Long>>>> simonInIn = Map.of("referred", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> simonOutOut = null;
        Map<String, List<Map<String, List<Long>>>> simonInOut = Map.of("referred", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> simonOutIn = null;

        Map<String, List<Set<VertexProperty>>> grantOutVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Set<VertexProperty>>> grantInVP = Map.of("worksWith", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Map<String, List<Long>>>> grantInIn = Map.of("worksWith", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> grantOutOut = Map.of("worksWith", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> grantInOut = Map.of("worksWith", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> grantOutIn = Map.of("worksWith", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));

        Map<String, List<Set<VertexProperty>>> joeOutVP = Map.of("manages", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Set<VertexProperty>>> joeInVP = Map.of("managedBy", List.of(Set.of(lyndonName, lyndonPosition)));
        Map<String, List<Map<String, List<Long>>>> joeInIn = Map.of("managedBy", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> joeOutOut = Map.of("manages", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> joeInOut = Map.of("managedBy", List.of(
                Map.of("managedBy", List.of(NumericIdManager.convert(lyndonManagedByJoe.id())),
                       "worksWith", List.of(NumericIdManager.convert(lyndonWorksWithGrant.id())),
                       "referred", List.of(NumericIdManager.convert(lyndonReferredSimon.id())))
        ));
        Map<String, List<Map<String, List<Long>>>> joeOutIn = Map.of("manages", List.of(
                Map.of("manages", List.of(NumericIdManager.convert(joeManagesLyndon.id())),
                       "worksWith", List.of(NumericIdManager.convert(grantWorksWithLyndon.id())),
                       "referred", List.of(NumericIdManager.convert(ishaanReferredLyndon.id())))
        ));

        g.V().drop().iterate();

        validateVertexProperty(Map.of(), db.OUT_VP_SET, lyndon);
        validateVertexProperty(Map.of(), db.IN_VP_SET, lyndon);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(lyndon, db.OUT_OUT_SET));

        validateVertexProperty(Map.of(), db.OUT_VP_SET, ishaan);
        validateVertexProperty(null, db.IN_VP_SET, ishaan);
        Assert.assertEquals(null, getCompoundEdgeMap(ishaan, db.IN_IN_SET));
        Assert.assertEquals(null, getCompoundEdgeMap(ishaan, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(ishaan, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(ishaan, db.OUT_OUT_SET));

        validateVertexProperty(null, db.OUT_VP_SET, simon);
        validateVertexProperty(Map.of(), db.IN_VP_SET, simon);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(simon, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(simon, db.IN_OUT_SET));
        Assert.assertEquals(null, getCompoundEdgeMap(simon, db.OUT_IN_SET));
        Assert.assertEquals(null, getCompoundEdgeMap(simon, db.OUT_OUT_SET));

        validateVertexProperty(Map.of(), db.OUT_VP_SET, grant);
        validateVertexProperty(Map.of(), db.IN_VP_SET, grant);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(grant, db.OUT_OUT_SET));

        validateVertexProperty(Map.of(), db.OUT_VP_SET, joe);
        validateVertexProperty(Map.of(), db.IN_VP_SET, joe);
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.IN_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.IN_OUT_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.OUT_IN_SET));
        Assert.assertEquals(Map.of(), getCompoundEdgeMap(joe, db.OUT_OUT_SET));
    }

    // This function is very useful when debugging the star data model.
    void dumpInfo(final Vertex vertex) {
        System.out.println("Vertex: " + vertex.property("name"));
        final FireflyRecord outProperties = FireflyRecord.read(db, db.OUT_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(vertex.id())));
        System.out.println("\tOut Properties:");
        if (outProperties != null) {
            System.out.println("\t\t " + outProperties.record.getValue(db.VERTEX_PROPERTY_NAME_TO_VALUE));
        }
        final FireflyRecord inProperties = FireflyRecord.read(db, db.IN_VP_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(vertex.id())));
        System.out.println("\tIn Properties:");
        if (inProperties != null) {
            System.out.println("\t\t " + inProperties.record.getValue(db.VERTEX_PROPERTY_NAME_TO_VALUE));
        }
        final FireflyRecord inIn = FireflyRecord.read(db, db.IN_IN_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(vertex.id())));
        System.out.println("\tIn in:");
        if (inIn != null) {
            System.out.println("\t\t " + inIn.record.getValue(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN));
        }
        final FireflyRecord outOut = FireflyRecord.read(db, db.OUT_OUT_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(vertex.id())));
        System.out.println("\tOut out:");
        if (outOut != null) {
            System.out.println("\t\t " + outOut.record.getValue(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN));
        }
        final FireflyRecord inOut = FireflyRecord.read(db, db.IN_OUT_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(vertex.id())));
        System.out.println("\tIn out:");
        if (inOut != null) {
            System.out.println("\t\t " + inOut.record.getValue(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN));
        }
        final FireflyRecord outIn = FireflyRecord.read(db, db.OUT_IN_SET, FireflyId.of(FireflyVertex.class, NumericIdManager.convert(vertex.id())));
        System.out.println("\tOut in:");
        if (outIn != null) {
            System.out.println("\t\t " + outIn.record.getValue(db.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN));
        }
    }
}
