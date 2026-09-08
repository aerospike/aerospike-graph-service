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

package com.aerospike.firefly.structure;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestGeoVertexPropertiesIntegration {
    private static final double SF_LON = -122.4194;
    private static final double SF_LAT = 37.7749;
    private static final double NEARBY_LON = -122.4175;
    private static final double NEARBY_LAT = 37.7755;
    private static final double OAKLAND_LON = -122.2711;
    private static final double OAKLAND_LAT = 37.8044;

    private static final String SUPERNODE_GRAPH_ID = "97";

    private static FireflyGraph graph;
    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED, "false");
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }

    @AfterClass
    public static void tearDown() {
        if (graph != null) {
            graph.getBaseGraph().dropDatabase(graph, true);
            graph.close();
        }
    }

    @Before
    public void before() {
        g.V().drop().iterate();
        graph.fireflyIndexMetadata.updateMetadata();
    }

    @Test
    public void writeReadRoundTripStoresGeoDataBin() throws InterruptedException {
        final Vertex vertex = g.addV("cafe")
                .property("geoLocation", Arrays.asList(SF_LON, SF_LAT))
                .next();

        @SuppressWarnings("unchecked")
        final List<Double> roundTrip = (List<Double>) g.V(vertex.id()).values("geoLocation").next();
        Assert.assertEquals(2, roundTrip.size());
        Assert.assertEquals(SF_LON, roundTrip.get(0), 0.0001);
        Assert.assertEquals(SF_LAT, roundTrip.get(1), 0.0001);

        final Key recordKey = new Key(graph.getBaseGraph().getConfig().namespace,
                graph.getBaseGraph().getConfig().vertexAeroSet,
                Value.get(vertex.id()));
        final Record record = graph.getBaseGraph().read(recordKey, null);
        final Map<Long, List<Object>> geoData = (Map<Long, List<Object>>) record.getMap(
                graph.getBaseGraph().getConfig().geoDataBin);
        Assert.assertNotNull(geoData);
        Assert.assertEquals(1, geoData.size());
        final Object storedPoint = geoData.values().iterator().next().get(0);
        // A GEO2DSPHERE index only indexes GeoJSON particles, so the stored point must not be a plain string.
        Assert.assertTrue("Stored geo point should be a GeoJSON particle but was " + storedPoint.getClass(),
                storedPoint instanceof Value.GeoJSONValue);
        Assert.assertTrue(storedPoint.toString().contains("Point"));

        waitForGeoIndex("geoLocation");
    }

    @Test
    public void indexedRadiusQueryFindsNearbyVertex() throws InterruptedException {
        final Vertex sf = g.addV("near").property("geoLocation", Arrays.asList(SF_LON, SF_LAT)).next();
        g.addV("far").property("geoLocation", Arrays.asList(OAKLAND_LON, OAKLAND_LAT)).next();
        waitForGeoIndex("geoLocation");

        final List<Vertex> matches = g.V().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(sf.id(), matches.get(0).id());
    }

    /**
     * Geo points must be written as Aerospike GeoJSON values. Plain strings are written as STRING particles, which a
     * GEO2DSPHERE index silently declines to index, leaving a fully-built index with zero entries.
     */
    @Test
    public void geoIndexContainsEntriesForWrittenPoints() throws InterruptedException {
        g.addV("indexed").property("geoLocation", Arrays.asList(SF_LON, SF_LAT)).next();
        g.addV("indexed").property("geoLocation", Arrays.asList(OAKLAND_LON, OAKLAND_LAT)).next();
        waitForGeoIndex("geoLocation");

        @SuppressWarnings("unchecked")
        final Map<String, Long> status = (Map<String, Long>) Admin.INDEX
                .getStatusVertexPropertyIndex(graph, "geoLocation", IndexType.GEO2DSPHERE);
        Assert.assertEquals(Long.valueOf(100L), status.get("percent_complete"));
        Assert.assertTrue("Geo index reported " + status.get("total_entries")
                        + " entries; geo points are not being indexed.",
                status.get("total_entries") >= 2L);
    }

    @Test
    public void scanFallbackFindsVertexAfterGeoIndexDrop() throws InterruptedException {
        final Vertex sf = g.addV("scan").property("geoLocation", Arrays.asList(SF_LON, SF_LAT)).next();
        waitForGeoIndex("geoLocation");

        g.call("aerospike.graph.admin.index.drop")
                .with("element_type", "vertex")
                .with("property_key", "geoLocation")
                .with("index_type", "geo")
                .next();
        Thread.sleep(500);
        graph.fireflyIndexMetadata.updateMetadata();
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfos().stream()
                .noneMatch(info -> "geoLocation".equals(info.key) && IndexType.GEO2DSPHERE.equals(info.indexType)));

        final List<Vertex> matches = g.V().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(sf.id(), matches.get(0).id());
    }

    @Test
    public void exactCoordinateMatch() throws InterruptedException {
        final Vertex vertex = g.addV("exact")
                .property("geoLocation", Arrays.asList(SF_LON, SF_LAT))
                .next();
        waitForGeoIndex("geoLocation");

        final List<Vertex> matches = g.V().has("geoLocation", Arrays.asList(SF_LON, SF_LAT)).toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(vertex.id(), matches.get(0).id());
    }

    @Test
    public void pointReadWithGeoWithin() throws InterruptedException {
        final Vertex vertex = g.addV("pointRead")
                .property("geoLocation", Arrays.asList(SF_LON, SF_LAT))
                .next();
        waitForGeoIndex("geoLocation");

        final List<Vertex> matches = g.V(vertex.id()).has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(vertex.id(), matches.get(0).id());
    }

    @Test
    public void multiPointSetMatchesWhenAnyPointWithinRadius() throws InterruptedException {
        final Vertex vertex = g.addV("multi")
                .property(VertexProperty.Cardinality.set, "geoLocation", Arrays.asList(SF_LON, SF_LAT))
                .property(VertexProperty.Cardinality.set, "geoLocation", Arrays.asList(NEARBY_LON, NEARBY_LAT))
                .next();
        waitForGeoIndex("geoLocation");

        final List<Vertex> matches = g.V().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(vertex.id(), matches.get(0).id());
        Assert.assertEquals(2, g.V(vertex.id()).values("geoLocation").toList().size());
    }

    /**
     * Geo properties live in a separate bin, so they have to be added explicitly to the key set that backs keys(),
     * valueMap() and no-argument properties().
     */
    @Test
    public void keylessAccessIncludesGeoProperty() {
        final Vertex vertex = g.addV("keyless")
                .property("geoLocation", Arrays.asList(SF_LON, SF_LAT))
                .property("name", "keylessCafe")
                .next();

        Assert.assertTrue("keys() omitted the geo property: " + g.V(vertex.id()).next().keys(),
                g.V(vertex.id()).next().keys().contains("geoLocation"));

        final Map<Object, Object> valueMap = g.V(vertex.id()).valueMap().next();
        Assert.assertTrue("valueMap() omitted the geo property: " + valueMap, valueMap.containsKey("geoLocation"));
        Assert.assertTrue(valueMap.containsKey("name"));
        Assert.assertEquals(2, g.V(vertex.id()).properties().count().next().intValue());
    }

    /**
     * A single write can carry the same property key more than once. The non-geo path keeps every value, so the geo
     * path must not overwrite all but the last.
     */
    @Test
    public void multiValueGeoPropertyInSingleWriteKeepsEveryPoint() {
        final Vertex vertex = graph.addVertex(
                "geoLocation", Arrays.asList(SF_LON, SF_LAT),
                "geoLocation", Arrays.asList(OAKLAND_LON, OAKLAND_LAT));

        Assert.assertEquals(2, g.V(vertex.id()).values("geoLocation").toList().size());
    }

    /**
     * Projecting a geo property alongside an ordinary one must return both, without falling back to reading every
     * property bin in full.
     */
    @Test
    public void projectedReadReturnsGeoAndNonGeoProperties() {
        final Vertex vertex = g.addV("projected")
                .property("geoLocation", Arrays.asList(SF_LON, SF_LAT))
                .property("name", "projectedCafe")
                .property("unused", "ignoreMe")
                .next();

        final Map<Object, Object> valueMap = g.V(vertex.id()).valueMap("geoLocation", "name").next();
        Assert.assertTrue("Projected read omitted the geo property: " + valueMap, valueMap.containsKey("geoLocation"));
        Assert.assertTrue(valueMap.containsKey("name"));
        Assert.assertFalse(valueMap.containsKey("unused"));
    }

    /**
     * or() of two radius predicates must match a vertex in either circle.
     */
    @Test
    public void connectiveGeoPredicateMatchesEitherBranch() {
        g.addV("connective").property("geoLocation", Arrays.asList(SF_LON, SF_LAT)).next();
        g.addV("connective").property("geoLocation", Arrays.asList(OAKLAND_LON, OAKLAND_LAT)).next();

        final List<Vertex> matches = g.V().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)
                .or(P.within(OAKLAND_LON, OAKLAND_LAT, 1000))).toList();
        Assert.assertEquals(2, matches.size());
    }

    /**
     * A region query must give the same answer whether Aerospike evaluates it through the geo index or the client
     * evaluates it during a scan.
     */
    @Test
    public void regionQueryAgreesBetweenIndexAndScan() throws InterruptedException {
        final Vertex sf = g.addV("region").property("geoLocation", Arrays.asList(SF_LON, SF_LAT)).next();
        g.addV("region").property("geoLocation", Arrays.asList(OAKLAND_LON, OAKLAND_LAT)).next();
        waitForGeoIndex("geoLocation");

        final List<Vertex> indexed = g.V().has("geoLocation", P.within(
                Arrays.asList(-122.52, 37.70),
                Arrays.asList(-122.35, 37.70),
                Arrays.asList(-122.35, 37.83),
                Arrays.asList(-122.52, 37.83),
                Arrays.asList(-122.52, 37.70))).toList();
        Assert.assertEquals(1, indexed.size());
        Assert.assertEquals(sf.id(), indexed.get(0).id());

        g.call("aerospike.graph.admin.index.drop")
                .with("element_type", "vertex")
                .with("property_key", "geoLocation")
                .with("index_type", "geo")
                .iterate();
        graph.fireflyIndexMetadata.updateMetadata();

        final List<Vertex> scanned = g.V().has("geoLocation", P.within(
                Arrays.asList(-122.52, 37.70),
                Arrays.asList(-122.35, 37.70),
                Arrays.asList(-122.35, 37.83),
                Arrays.asList(-122.52, 37.83),
                Arrays.asList(-122.52, 37.70))).toList();
        Assert.assertEquals("Index and scan disagree for the same region query", 1, scanned.size());
        Assert.assertEquals(sf.id(), scanned.get(0).id());
    }

    @Test
    public void geoIdIntegerPropertyDoesNotUseGeoRadiusRewrite() {
        g.addV("legacy").property("geoId", 2).next();
        final List<Vertex> matches = g.V().has("geoId", P.within(1, 2, 3)).toList();
        Assert.assertEquals(1, matches.size());
    }

    @Test
    public void adminGeoIndexCreateAndList() throws InterruptedException {
        g.addV("admin").property("geoLocation", Arrays.asList(SF_LON, SF_LAT)).next();
        waitForGeoIndex("geoLocation");

        @SuppressWarnings("unchecked")
        final List<String> indexes = (List<String>) g.call("aerospike.graph.admin.index.list").next();
        Assert.assertTrue(indexes.contains("geoLocation:GEO2DSPHERE"));

        g.call("aerospike.graph.admin.index.drop")
                .with("element_type", "vertex")
                .with("property_key", "geoLocation")
                .with("index_type", "geo")
                .next();
        Thread.sleep(500);
        graph.fireflyIndexMetadata.updateMetadata();

        g.call("aerospike.graph.admin.index.create")
                .with("element_type", "vertex")
                .with("property_key", "geoLocation")
                .with("index_type", "geo")
                .next();
        waitForGeoIndex(graph, "geoLocation");
    }

    @Test
    public void midTraversalHasAfterOutFindsNearbyVertex() throws InterruptedException {
        final HubSpoke hubSpoke = seedHubSpoke();

        final List<Vertex> matches = g.V(hubSpoke.hub().id()).out().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(hubSpoke.sf().id(), matches.get(0).id());
    }

    @Test
    public void midTraversalHasAfterInVFindsNearbyVertex() throws InterruptedException {
        final HubSpoke hubSpoke = seedHubSpoke();

        final List<Vertex> matches = g.V(hubSpoke.hub().id()).outE().inV().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(hubSpoke.sf().id(), matches.get(0).id());
    }

    @Test
    public void midTraversalHasAfterOtherVFindsNearbyVertex() throws InterruptedException {
        final HubSpoke hubSpoke = seedHubSpoke();

        final List<Vertex> matches = g.V(hubSpoke.hub().id()).outE().otherV().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(hubSpoke.sf().id(), matches.get(0).id());
    }

    @Test
    public void midTraversalWhereHasFindsNearbyVertex() throws InterruptedException {
        final HubSpoke hubSpoke = seedHubSpoke();

        final List<Vertex> matches = g.V(hubSpoke.hub().id()).out()
                .where(__.has("geoLocation", P.within(SF_LON, SF_LAT, 1000)))
                .toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(hubSpoke.sf().id(), matches.get(0).id());
    }

    @Test
    public void midTraversalValuesAfterOutReturnsGeoCoordinates() throws InterruptedException {
        final HubSpoke hubSpoke = seedHubSpoke();

        final List<Object> values = g.V(hubSpoke.hub().id()).out().values("geoLocation").toList();
        Assert.assertEquals(2, values.size());
        Assert.assertTrue(values.stream().anyMatch(value -> coordinatesNear(value, SF_LON, SF_LAT)));
        Assert.assertTrue(values.stream().anyMatch(value -> coordinatesNear(value, OAKLAND_LON, OAKLAND_LAT)));
    }

    @Test
    public void midTraversalHasAfterOutEOutVFindsNearbyVertex() throws InterruptedException {
        final HubSpoke hubSpoke = seedHubSpoke();

        final List<Vertex> matches = g.V(hubSpoke.sf().id()).inE("near").outV().out("near")
                .has("geoLocation", P.within(SF_LON, SF_LAT, 1000))
                .toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(hubSpoke.sf().id(), matches.get(0).id());
    }

    @Test
    public void supernodeVertexReadsAndFiltersGeoProperty() throws InterruptedException {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED, "false");
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "2");
        // Its own graph id: this graph gets dropped, and dropping truncates the schema set that the graph id scopes.
        config.setProperty(ConfigurationHelper.Keys.GRAPH_ID, SUPERNODE_GRAPH_ID);
        try (FireflyGraph supernodeGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource sg = supernodeGraph.traversal();
            final Vertex hub = sg.addV("hub")
                    .property("geoLocation", Arrays.asList(SF_LON, SF_LAT))
                    .next();
            for (int i = 0; i < 3; i++) {
                final Vertex leaf = sg.addV("leaf").property("idx", i).next();
                sg.V(hub.id()).addE("link").to(leaf).iterate();
            }

            final FireflyVertex reloadedHub = (FireflyVertex) sg.V(hub.id()).next();
            Assert.assertTrue(reloadedHub.isEdgeCacheOverflowed());

            waitForGeoIndex(supernodeGraph, "geoLocation");

            @SuppressWarnings("unchecked")
            final List<Double> roundTrip = (List<Double>) sg.V(hub.id()).values("geoLocation").next();
            Assert.assertEquals(SF_LON, roundTrip.get(0), 0.0001);
            Assert.assertEquals(SF_LAT, roundTrip.get(1), 0.0001);

            final List<Vertex> matches = sg.V().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
            Assert.assertEquals(1, matches.size());
            Assert.assertEquals(hub.id(), matches.get(0).id());

            supernodeGraph.getBaseGraph().dropDatabase(supernodeGraph, true);
        }
    }

    @Test
    public void supernodeHubOutStepFiltersGeoOnAdjacentVertices() throws InterruptedException {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED, "false");
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "2");
        // Its own graph id: this graph gets dropped, and dropping truncates the schema set that the graph id scopes.
        config.setProperty(ConfigurationHelper.Keys.GRAPH_ID, SUPERNODE_GRAPH_ID);
        try (FireflyGraph supernodeGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource sg = supernodeGraph.traversal();
            final Vertex hub = sg.addV("hub").property("name", "hub").next();
            final Vertex sf = sg.addV("leaf").property("geoLocation", Arrays.asList(SF_LON, SF_LAT)).next();
            final Vertex oakland = sg.addV("leaf").property("geoLocation", Arrays.asList(OAKLAND_LON, OAKLAND_LAT)).next();
            sg.V(hub.id()).addE("near").to(sf).iterate();
            sg.V(hub.id()).addE("near").to(oakland).iterate();
            sg.V(hub.id()).addE("extra").to(sg.addV("leaf").property("idx", 1).next()).iterate();

            Assert.assertTrue(((FireflyVertex) sg.V(hub.id()).next()).isEdgeCacheOverflowed());
            waitForGeoIndex(supernodeGraph, "geoLocation");

            final List<Vertex> matches = sg.V(hub.id()).out().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
            Assert.assertEquals(1, matches.size());
            Assert.assertEquals(sf.id(), matches.get(0).id());

            supernodeGraph.getBaseGraph().dropDatabase(supernodeGraph, true);
        }
    }

    @Test
    public void explicitSupernodeFlagReadsAndFiltersGeoProperty() throws InterruptedException {
        final Vertex hub = g.addV("hub")
                .property(FireflyVertex.SUPERNODE_PROPERTY_KEY, true)
                .property("geoLocation", Arrays.asList(SF_LON, SF_LAT))
                .next();
        waitForGeoIndex(graph, "geoLocation");

        Assert.assertTrue(((FireflyVertex) g.V(hub.id()).next()).isEdgeCacheOverflowed());

        @SuppressWarnings("unchecked")
        final List<Double> roundTrip = (List<Double>) g.V(hub.id()).values("geoLocation").next();
        Assert.assertEquals(SF_LON, roundTrip.get(0), 0.0001);
        Assert.assertEquals(SF_LAT, roundTrip.get(1), 0.0001);

        final List<Vertex> matches = g.V().has("geoLocation", P.within(SF_LON, SF_LAT, 1000)).toList();
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(hub.id(), matches.get(0).id());
    }

    private static HubSpoke seedHubSpoke() throws InterruptedException {
        final Vertex hub = g.addV("place").property("name", "hub").next();
        final Vertex sf = g.addV("place").property("geoLocation", Arrays.asList(SF_LON, SF_LAT)).next();
        final Vertex oakland = g.addV("place").property("geoLocation", Arrays.asList(OAKLAND_LON, OAKLAND_LAT)).next();
        g.V(hub.id()).addE("near").to(sf).iterate();
        g.V(hub.id()).addE("near").to(oakland).iterate();
        waitForGeoIndex(graph, "geoLocation");
        return new HubSpoke(hub, sf, oakland);
    }

    private static boolean coordinatesNear(final Object value, final double expectedLon, final double expectedLat) {
        if (!(value instanceof List)) {
            return false;
        }
        final List<?> coordinates = (List<?>) value;
        if (coordinates.size() != 2) {
            return false;
        }
        return Math.abs(((Number) coordinates.get(0)).doubleValue() - expectedLon) < 0.0001
                && Math.abs(((Number) coordinates.get(1)).doubleValue() - expectedLat) < 0.0001;
    }

    private static final class HubSpoke {
        private final Vertex hub;
        private final Vertex sf;
        private final Vertex oakland;

        private HubSpoke(final Vertex hub, final Vertex sf, final Vertex oakland) {
            this.hub = hub;
            this.sf = sf;
            this.oakland = oakland;
        }

        private Vertex hub() {
            return hub;
        }

        private Vertex sf() {
            return sf;
        }

        private Vertex oakland() {
            return oakland;
        }
    }

    private static void waitForGeoIndex(final String propertyKey) throws InterruptedException {
        waitForGeoIndex(graph, propertyKey);
    }

    private static void waitForGeoIndex(final FireflyGraph targetGraph, final String propertyKey) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            targetGraph.fireflyIndexMetadata.updateMetadata();
            final FireflyIndexMetadata.IndexInfo info = targetGraph.fireflyIndexMetadata.getPropertyIndexInfos().stream()
                    .filter(indexInfo -> propertyKey.equals(indexInfo.key)
                            && IndexType.GEO2DSPHERE.equals(indexInfo.indexType))
                    .findFirst()
                    .orElse(null);
            if (info != null) {
                try {
                    @SuppressWarnings("unchecked")
                    final Map<String, Long> status = (Map<String, Long>) Admin.INDEX
                            .getStatusVertexPropertyIndex(targetGraph, propertyKey, IndexType.GEO2DSPHERE);
                    if (status.get("percent_complete") == 100L) {
                        return;
                    }
                } catch (final IllegalStateException ignored) {
                    // Index still building.
                }
            }
            Thread.sleep(100);
        }
        Assert.fail("Timed out waiting for geo index on property " + propertyKey);
    }
}
