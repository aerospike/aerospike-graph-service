package com.aerospike.firefly.io.impl.relational.star.packed;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.google.common.collect.ImmutableList;
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
    public void afterTest() throws InterruptedException {
        // Use this to clear data here because we haven't solidified our deletes yet.
        graph.getBaseGraph().dropDatabase();
        Thread.sleep(1000);
    }

    @Test
    // I don't usually like behemoth tests like this, but there isn't really an option here
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

    void validateVertexProperty(Map<String, List<Set<VertexProperty>>> expectedProperties, final String set, final Vertex vertex) {
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
}
