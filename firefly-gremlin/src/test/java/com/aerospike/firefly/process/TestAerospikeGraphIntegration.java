package com.aerospike.firefly.process;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.collect.ImmutableList;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.FailStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.EDGE_CACHE_DISABLED_GLOBALLY;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.Merge.onCreate;
import static org.apache.tinkerpop.gremlin.process.traversal.Merge.onMatch;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.Pick.none;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.V;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.properties;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.apache.tinkerpop.gremlin.structure.Column.values;
import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeGraphIntegration extends AbstractFireflySuite {
    Logger logger = LoggerFactory.getLogger(TestAerospikeGraphIntegration.class);
    GraphTraversalSource g;

    public void printTraversalForm(final Traversal traversal) {
        logger.info("   pre-strategy:" + traversal);
        if (!traversal.asAdmin().isLocked()) traversal.asAdmin().applyStrategies();
        logger.info("  post-strategy:" + traversal);
    }

    @Before
    public void setupTraversal() {
        g = graph.traversal();
    }


    // 1 kB string.
    private static final int STRING_LENGTH = 1000;
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final Random RANDOM = new Random();
    private static final int MAX_SIZE = 10 * 1000;
    private static final int PROPERTY_COUNT = 1020;
    private static final String RANDOM_STRING;

    static {
        final StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.length() < STRING_LENGTH) {
            int idx = (int) (RANDOM.nextFloat() * CHARACTERS.length());
            stringBuilder.append(CHARACTERS.charAt(idx));
        }
        RANDOM_STRING = stringBuilder.toString();
    }

    @Test
    public void g_V_out_out_path_byXnameX_byXageX() {
        Graph tg = TinkerFactory.createModern();

        GraphHelper.cloneElements(tg, graph);
        GraphTraversalSource g = graph.traversal();
        Traversal<Vertex, org.apache.tinkerpop.gremlin.process.traversal.Path> traversal =
                g.V().out().out().path().by("name").by("age");

        this.printTraversalForm(traversal);
        int counter = 0;

        while (traversal.hasNext()) {
            ++counter;
            org.apache.tinkerpop.gremlin.process.traversal.Path path = (Path) traversal.next();
            assertEquals(3L, (long) path.size());
            assertEquals("marko", path.get(0));
            assertEquals(32, (int) path.get(1));
            assertTrue(path.get(2).equals("lop") || path.get(2).equals("ripple"));
        }

        assertEquals(2L, (long) counter);
    }

    @Test
    public void g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);

        final Traversal<Vertex, Vertex> traversal = g.addV("person")
                .property(VertexProperty.Cardinality.single, "name", "stephen")
                .property(VertexProperty.Cardinality.single, "name", "stephenm", "since", 2010);
        printTraversalForm(traversal);
        final Vertex stephen = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        assertEquals("person", stephen.label());
        assertEquals("stephenm", stephen.value("name"));
        assertEquals(2010, Integer.parseInt(stephen.property("name").value("since").toString()));
        assertEquals(1, FireflyCloseableIteratorUtils.count(stephen.property("name").properties()));
        assertEquals(1, FireflyCloseableIteratorUtils.count(stephen.properties()));
        assertEquals(7, FireflyCloseableIteratorUtils.count(g.V()));
    }

    @Test
    public void g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        final Traversal<Vertex, Vertex> traversal = g.addV("person").property(VertexProperty.Cardinality.single, "name", "stephen").property(VertexProperty.Cardinality.single, "name", "stephenm");
        printTraversalForm(traversal);
        final Vertex stephen = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        assertEquals("person", stephen.label());
        assertEquals("stephenm", stephen.value("name"));
        assertEquals(1, FireflyCloseableIteratorUtils.count(stephen.properties()));
        assertEquals(7, FireflyCloseableIteratorUtils.count(g.V()));
    }

    @Test
    public void g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX() {
        final Traversal<Edge, Edge> traversal = g.mergeE(asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100))).
                option(onCreate, asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100), "created", "Y")).
                option(onMatch, asMap("created", "N"));
        printTraversalForm(traversal);
        try {
            traversal.next();
            Assert.fail("Should have failed as vertices are not created");
        } catch (Exception ex) {
            assertThat(ex.getMessage(), endsWith("could not be found and edge could not be created"));
        }
        assertEquals(0, FireflyCloseableIteratorUtils.count(g.E()));
    }

    private static void assertId(final Graph g, final boolean lossyForId, final Element e, final Object expected) {
        // it is possible that a Graph (e.g. elastic-gremlin) can supportUserSuppliedIds but internally
        // represent them as a value other than Numeric (which is what's in all of the test/toy data).
        // as we feature check for userSuppliedIds when asserting the identifier, we also ensure that
        // the id can be properly asserted for that Element before attempting to do so.  By asserting
        // at this level in this way, graphs can enjoy greater test coverage in IO.
        if ((e instanceof Vertex && g.features().vertex().supportsUserSuppliedIds() && g.features().vertex().supportsNumericIds())
                || (e instanceof Edge && g.features().edge().supportsUserSuppliedIds() && g.features().edge().supportsNumericIds())
                || (e instanceof VertexProperty && g.features().vertex().properties().supportsUserSuppliedIds()) && g.features().vertex().properties().supportsNumericIds()) {
            if (lossyForId)
                assertEquals(expected.toString(), e.id().toString());
            else
                assertEquals(expected, e.id());
        }
    }

    private static void assertWeightLoosely(final double expected, final Edge e) {
        try {
            assertEquals(expected, e.value("weight"), 0.0001d);
        } catch (Exception ex) {
            // for graphs that have strong typing via schema it is possible that a value that came across as graphson
            // with lossiness will end up having a value expected to double to be coerced to float by the underlying
            // graph.
            assertEquals(new Double(expected).floatValue(), e.value("weight"), 0.0001f);
        }
    }

    @Test
    public void propertyTest() {
        g.addV("something").property("a", "b").property("c", "d").next();
        Vertex it = g.V().has("a", "b").next();
        Map<String, Object> stuff = g.V().has("a", "b").propertyMap().next();
        List<Map<String, Object>> bulkproperties = g.V().propertyMap().toList();
        assertNotNull(it);
    }

    private static void assertToyGraph(final Graph g1, final boolean assertDouble, final boolean lossyForId, final boolean assertSpecificLabel) {
        assertEquals(6, FireflyCloseableIteratorUtils.count(g1.vertices()));
        var l = ImmutableList.copyOf(g1.edges());
        assertEquals(6, FireflyCloseableIteratorUtils.count(g1.edges()));
        List<Vertex> stuff = g1.traversal().V().toList();
        List<Map<String, Object>> stuff2 = g1.traversal().V().propertyMap().toList();
        final Vertex v1 = g1.traversal().V().has("name", "marko").next();
        assertEquals(29, v1.<Integer>value("age").intValue());
        assertEquals(2, v1.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v1.label());
        assertId(g1, lossyForId, v1, 1);

        final List<Edge> v1Edges = FireflyCloseableIteratorUtils.list(v1.edges(Direction.BOTH));
        assertEquals(3, v1Edges.size());
        v1Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("vadas")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.5d, e);
                else
                    assertWeightLoosely(0.5f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else if (e.inVertex().value("name").equals("josh")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertWeightLoosely(1.0, e);
                else
                    assertWeightLoosely(1.0f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else if (e.inVertex().value("name").equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.4d, e);
                else
                    assertWeightLoosely(0.4f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 9);
            } else {
                Assert.fail("Edge not expected");
            }
        });

        final Vertex v2 = g1.traversal().V().has("name", "vadas").next();
        assertEquals(27, v2.<Integer>value("age").intValue());
        assertEquals(2, v2.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v2.label());
        assertId(g1, lossyForId, v2, 2);

        final List<Edge> v2Edges = FireflyCloseableIteratorUtils.list(v2.edges(Direction.BOTH));
        assertEquals(1, v2Edges.size());
        v2Edges.forEach(e -> {
            if (e.outVertex().value("name").equals("marko")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.5d, e);
                else
                    assertWeightLoosely(0.5f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 7);
            } else {
                Assert.fail("Edge not expected");
            }
        });

        final Vertex v3 = g1.traversal().V().has("name", "lop").next();
        assertEquals("java", v3.<String>value("lang"));
        assertEquals(2, v2.keys().size());
        assertEquals(assertSpecificLabel ? "software" : Vertex.DEFAULT_LABEL, v3.label());
        assertId(g1, lossyForId, v3, 3);

        final List<Edge> v3Edges = FireflyCloseableIteratorUtils.list(v3.edges(Direction.BOTH));
        assertEquals(3, v3Edges.size());
        v3Edges.forEach(e -> {
            if (e.outVertex().value("name").equals("peter")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.2d, e);
                else
                    assertWeightLoosely(0.2f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 12);
            } else if (e.outVertex().value("name").equals("josh")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.4d, e);
                else
                    assertWeightLoosely(0.4f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outVertex().value("name").equals("marko")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.4d, e);
                else
                    assertWeightLoosely(0.4f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 9);
            } else {
                Assert.fail("Edge not expected");
            }
        });

        final Vertex v4 = g1.traversal().V().has("name", "josh").next();
        assertEquals(32, v4.<Integer>value("age").intValue());
        assertEquals(2, v4.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v4.label());
        assertId(g1, lossyForId, v4, 4);

        final List<Edge> v4Edges = FireflyCloseableIteratorUtils.list(v4.edges(Direction.BOTH));
        assertEquals(3, v4Edges.size());
        v4Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("ripple")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(1.0d, e);
                else
                    assertWeightLoosely(1.0f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else if (e.inVertex().value("name").equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.4d, e);
                else
                    assertWeightLoosely(0.4f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 11);
            } else if (e.outVertex().value("name").equals("marko")) {
                assertEquals("knows", e.label());
                if (assertDouble)
                    assertWeightLoosely(1.0d, e);
                else
                    assertWeightLoosely(1.0f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 8);
            } else {
                Assert.fail("Edge not expected");
            }
        });

        final Vertex v5 = g1.traversal().V().has("name", "ripple").next();
        assertEquals("java", v5.<String>value("lang"));
        assertEquals(2, v5.keys().size());
        assertEquals(assertSpecificLabel ? "software" : Vertex.DEFAULT_LABEL, v5.label());
        assertId(g1, lossyForId, v5, 5);

        final List<Edge> v5Edges = FireflyCloseableIteratorUtils.list(v5.edges(Direction.BOTH));
        assertEquals(1, v5Edges.size());
        v5Edges.forEach(e -> {
            if (e.outVertex().value("name").equals("josh")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(1.0d, e);
                else
                    assertWeightLoosely(1.0f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 10);
            } else {
                Assert.fail("Edge not expected");
            }
        });

        final Vertex v6 = g1.traversal().V().has("name", "peter").next();
        assertEquals(35, v6.<Integer>value("age").intValue());
        assertEquals(2, v6.keys().size());
        assertEquals(assertSpecificLabel ? "person" : Vertex.DEFAULT_LABEL, v6.label());
        assertId(g1, lossyForId, v6, 6);

        final List<Edge> v6Edges = FireflyCloseableIteratorUtils.list(v6.edges(Direction.BOTH));
        assertEquals(1, v6Edges.size());
        v6Edges.forEach(e -> {
            if (e.inVertex().value("name").equals("lop")) {
                assertEquals("created", e.label());
                if (assertDouble)
                    assertWeightLoosely(0.2d, e);
                else
                    assertWeightLoosely(0.2f, e);
                assertEquals(1, e.keys().size());
                assertId(g1, lossyForId, e, 12);
            } else {
                Assert.fail("Edge not expected");
            }
        });
    }

    @Test
    public void g_io_read_withXreader_graphsonX() throws IOException {
        String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GraphSONResourceAccess.class, "tinkerpop-modern-v3d0.json", "").getAbsolutePath().replace('\\', '/');
        Traversal<Object, Object> traversal = g.io(fileToRead).with(IO.reader, IO.graphson).read();
        this.printTraversalForm(traversal);
        traversal.iterate();
        assertToyGraph(this.graph, false, true, true);
    }

    @Test
    public void g_io_read_withXreader_gryoX() throws IOException {
        String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GryoResourceAccess.class, "tinkerpop-modern-v3d0.kryo", "").getAbsolutePath().replace('\\', '/');
        Traversal<Object, Object> traversal = g.io(fileToRead).with(IO.reader, IO.gryo).read();
        this.printTraversalForm(traversal);
        traversal.iterate();

        IoTest.assertModernGraph(this.graph, false, true);

    }

    @Test
    public void g_io_read_withXreader_graphmlX() throws IOException {
        final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GraphMLResourceAccess.class, "tinkerpop-modern.xml", "").getAbsolutePath().replace('\\', '/');
        final Traversal<Object, Object> traversal = g.io(fileToRead).with(IO.reader, IO.graphml).read();
        printTraversalForm(traversal);
        traversal.iterate();
        Iterator<Vertex> vertices = graph.vertices();
        Iterator<Edge> edge = graph.edges();

        IoTest.assertModernGraph(graph, false, true);

    }

    @Test
    public void g_io_readXjsonX() throws IOException {
        final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GraphSONResourceAccess.class, "tinkerpop-modern-v3d0.json", "").getAbsolutePath().replace('\\', '/');
        final Traversal<Object, Object> traversal = g.io(fileToRead).read();
        printTraversalForm(traversal);
        traversal.iterate();

        IoTest.assertModernGraph(graph, false, true);

    }

    @Test
    public void g_io_readXkryoX() throws IOException {
        String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GryoResourceAccess.class, "tinkerpop-modern-v3d0.kryo", "").getAbsolutePath().replace('\\', '/');
        Traversal<Object, Object> traversal = g.io(fileToRead).read();
        this.printTraversalForm(traversal);
        traversal.iterate();

        IoTest.assertModernGraph(this.graph, false, true);
    }

    public static void assertModernGraph(final Graph g1, final boolean assertDouble, final boolean lossyForId) {
        assertToyGraph(g1, assertDouble, lossyForId, true);
    }

    @Test
    public void g_io_readXxmlX() throws IOException {
        final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GraphMLResourceAccess.class, "tinkerpop-modern.xml", "").getAbsolutePath().replace('\\', '/');
        final Traversal<Object, Object> traversal = g.io(fileToRead).read();
        printTraversalForm(traversal);
        traversal.iterate();

        assertModernGraph(graph, false, true);

    }

    @Test
    public void shouldDetachVertexPropertyWhenChanged() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final String label = v.label();
        final Object id = v.id();
        v.property("to-change", "blah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyChanged(final Vertex element, final VertexProperty oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
                assertThat(element, instanceOf(DetachedVertex.class));
                assertEquals(label, element.label());
                assertEquals(id, element.id());
                assertEquals("to-change", oldValue.key());
                assertEquals("blah", oldValue.value());
                assertEquals("dah", setValue);
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).property(VertexProperty.Cardinality.single, "to-change", "dah").iterate();
        tryCommit(graph);

        assertEquals(1, FireflyCloseableIteratorUtils.count(g.V(v).properties()));
        assertThat(triggered.get(), is(true));
    }

    @Test
    public void shouldUseActualVertexPropertyWhenChanged() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final String label = v.label();
        final Object id = v.id();
        v.property("to-change", "blah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyChanged(final Vertex element, final VertexProperty oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
                assertEquals(v, element);
                assertEquals(label, element.label());
                assertEquals(id, element.id());
                assertEquals("to-change", oldValue.key());
                assertEquals("blah", oldValue.value());
                assertEquals("dah", setValue);
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener).detach(EventStrategy.Detachment.REFERENCE);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).property(VertexProperty.Cardinality.single, "to-change", "dah").iterate();
        tryCommit(graph);

        assertEquals(1, FireflyCloseableIteratorUtils.count(g.V(v).properties()));
        assertThat(triggered.get(), is(true));
    }

    @Test
    public void shouldReferenceVertexPropertyWhenChanged() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final String label = v.label();
        final Object id = v.id();
        v.property("to-change", "blah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyChanged(final Vertex element, final VertexProperty oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
                assertThat(element, instanceOf(ReferenceVertex.class));
                assertEquals(label, element.label());
                assertEquals(id, element.id());
                assertEquals("to-change", oldValue.key());
                assertEquals("blah", oldValue.value());
                assertEquals("dah", setValue);
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener).detach(EventStrategy.Detachment.REFERENCE);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).property(VertexProperty.Cardinality.single, "to-change", "dah").iterate();
        tryCommit(graph);

        assertEquals(1, FireflyCloseableIteratorUtils.count(g.V(v).properties()));
        assertThat(triggered.get(), is(true));
    }

    public Edge convertToEdge(final Graph graph, final String outVertexName, String edgeLabel, final String inVertexName) {
        return graph.traversal().V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").toList().get(0);
    }

    public Edge convertToEdge(final String outVertexName, String edgeLabel, final String inVertexName) {
        return convertToEdge(graph, outVertexName, edgeLabel, inVertexName);
    }

    public static <T> void checkOrderedResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        Assert.assertFalse(traversal.hasNext());
        if (expectedResults.size() != results.size()) {
            assertEquals("Checking result size", expectedResults.size(), results.size());
        }
        for (int i = 0; i < expectedResults.size(); i++) {
            assertEquals(expectedResults.get(i), results.get(i));
        }
    }

    public void tryCommit(final Graph graph) {
        if (graph.features().graph().supportsTransactions())
            graph.tx().commit();
    }

    private GraphTraversalSource create(final EventStrategy strategy) {
        return traversal(graph, strategy);
    }

    public GraphTraversalSource traversal(final Graph graph, final TraversalStrategy... strategies) {
        return graph.traversal().withStrategies(strategies);
    }

    @Override
    protected boolean clearData() {
        return true;
    }

    static abstract class AbstractMutationListener implements MutationListener {
        @Override
        public void vertexAdded(final Vertex vertex) {

        }

        @Override
        public void vertexRemoved(final Vertex vertex) {

        }

        @Override
        public void vertexPropertyChanged(final Vertex element, final VertexProperty oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {

        }

        @Override
        public void vertexPropertyRemoved(final VertexProperty vertexProperty) {

        }

        @Override
        public void edgeAdded(final Edge edge) {

        }

        @Override
        public void edgeRemoved(final Edge edge) {

        }

        @Override
        public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {

        }

        @Override
        public void edgePropertyRemoved(final Edge element, final Property property) {

        }

        @Override
        public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {

        }

        @Override
        public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property property) {

        }
    }

    public Vertex convertToVertex(final Graph graph, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return graph.traversal().V().has("name", vertexName).toList().get(0);
    }

    public Object convertToVertexId(final Graph graph, final String vertexName) {
        return convertToVertex(graph, vertexName).id();
    }

    @Test
    public void g_addEXknowsX_fromXaX_toXbX_propertyXweight_0_1X() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);

        Vertex a = (Vertex) this.g.V(new Object[0]).has("name", "marko").next();
        Vertex b = (Vertex) this.g.V(new Object[0]).has("name", "peter").next();
        Traversal<Edge, Edge> traversal = g.addE("knows").from(a).to(b).property("weight", 0.1d);
        this.printTraversalForm(traversal);
        Edge edge = (Edge) traversal.next();
        assertEquals(edge.outVertex(), convertToVertex(this.graph, "marko"));
        assertEquals(edge.inVertex(), convertToVertex(this.graph, "peter"));
        assertEquals("knows", edge.label());
        assertEquals(1L, FireflyCloseableIteratorUtils.count(edge.properties(new String[0])));
        assertEquals(0.1, (Double) edge.value("weight"), 0.1);
        assertEquals(6L, this.g.V(new Object[0]).count().next().longValue());
        assertEquals(7L, this.g.E(new Object[0]).count().next().longValue());
    }

    @Test
    public void g_addEXV_outE_label_groupCount_orderXlocalX_byXvalues_descX_selectXkeysX_unfold_limitX1XX_fromXV_hasXname_vadasXX_toXV_hasXname_lopXX() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);

        Traversal<Edge, Edge> traversal = g.addE(V().outE().label().groupCount().order(local).by(values, desc).select(keys).<String>unfold().limit(1)).from(V().has("name", "vadas")).to(V().has("name", "lop"));
        this.printTraversalForm(traversal);
        Edge edge = (Edge) traversal.next();
        Assert.assertFalse(traversal.hasNext());
        assertEquals("created", edge.label());
        assertEquals(convertToVertexId(graph, "vadas"), edge.outVertex().id());
        assertEquals(convertToVertexId(graph, "lop"), edge.inVertex().id());
        assertEquals(6L, this.g.V(new Object[0]).count().next().longValue());
        assertEquals(7L, this.g.E(new Object[0]).count().next().longValue());
    }

    @Test
    public void g_withSideEffectXa_testX_V_hasLabelXsoftwareX_propertyXtemp_selectXaXX_valueMapXname_tempX() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        TinkerGraph tgraph = TinkerFactory.createModern();
        GraphTraversalSource tg = tgraph.traversal();

        Traversal<Vertex, Map<Object, List<String>>> tgtraversal = tg.withSideEffect("a", "test")
                .V().hasLabel("software")
                .property("temp", select("a"))
                .valueMap("name", "temp");

        Traversal<Vertex, Map<Object, List<String>>> traversal = g.withSideEffect("a", "test")
                .V().hasLabel("software")
                .property("temp", select("a"))
                .valueMap("name", "temp");
        this.printTraversalForm(traversal);
        int counter = 0;

        while (traversal.hasNext()) {
            ++counter;
            Map<Object, List<String>> valueMap = (Map) traversal.next();
            assertEquals(2L, (long) valueMap.size());
            assertEquals(Collections.singletonList("test"), valueMap.get("temp"));
            assertTrue(((List) valueMap.get("name")).equals(Collections.singletonList("ripple")) || ((List) valueMap.get("name")).equals(Collections.singletonList("lop")));
        }
        assertEquals(2L, (long) counter);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_hasXname_endingWithXasXX() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);

        Traversal<Vertex, Vertex> traversal = g.V().has("name", TextP.endingWith("as"));

        this.printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertTrue(((Vertex) traversal.next()).value("name").equals("vadas"));
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);

        Traversal<Vertex, Vertex> traversal = g.V(convertToVertexId(this.graph, "marko"))
                .as("a")
                .addV("animal")
                .property("age", select("a").by("age"))
                .property("name", "puppy");
        this.printTraversalForm(traversal);
        List<Vertex> things = traversal.toList();
        Vertex vertex = things.iterator().next();
        List<VertexProperty<Object>> stuff = FireflyCloseableIteratorUtils.list(vertex.properties());

        assertEquals("animal", vertex.label());
        assertEquals(29L, (long) (Integer) vertex.value("age"));
        assertEquals("puppy", vertex.value("name"));
        Assert.assertFalse(traversal.hasNext());
        assertEquals(7L, FireflyCloseableIteratorUtils.count(this.g.V(new Object[0])));
    }

    @Test
    public void g_V_outE_propertyXweight_nullX() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);

        Traversal<Vertex, Edge> traversal = g.V().outE().property("weight", null);
        this.printTraversalForm(traversal);
        traversal.forEachRemaining((e) -> {
            MatcherAssert.assertThat(e.properties(new String[]{"weight"}).hasNext(), Is.is(false));
        });
    }

    @Test
    public void g_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        TinkerGraph tgraph = TinkerFactory.createModern();
        GraphTraversalSource tg = tgraph.traversal();
        Map<String, Object> tgopm = tg.V().has("name", "marko").propertyMap().next();
        Map<String, Object> opm = g.V().has("name", "marko").propertyMap().next();

        Traversal<Vertex, Vertex> traversal =
                g.mergeV(asMap(T.label, "person", "name", "marko")).option(onMatch, asMap("age", 19));
        this.printTraversalForm(traversal);
        Vertex tgVertex = tg.mergeV(asMap(T.label, "person", "name", "marko")).option(onMatch, asMap("age", 19)).next();

        Vertex vertex = (Vertex) traversal.next();
        Map<String, Object> npm = g.V().has("name", "marko").propertyMap().next();
        Map<String, Object> tgnpm = tg.V().has("name", "marko").propertyMap().next();

        assertEquals("person", vertex.label());
        assertEquals("marko", vertex.value("name"));
        assertEquals(19L, (long) (Integer) vertex.value("age"));
        Assert.assertFalse(traversal.hasNext());
        assertEquals(6L, FireflyCloseableIteratorUtils.count(this.g.V(new Object[0])));
    }

    @Test
    public void g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        Map<String, Object> opm = g.V().has("name", "marko").propertyMap().next();

        Traversal<Object, Vertex> traversal = g.withSideEffect("c", asMap(T.label, "person", "name", "marko")).
                withSideEffect("m", asMap("age", 19)).
                mergeV(select("c")).option(onMatch, select("m"));
        this.printTraversalForm(traversal);
        Vertex vertex = (Vertex) traversal.next();
        Map<String, Object> npm = g.V().has("name", "marko").propertyMap().next();

        assertEquals("person", vertex.label());
        assertEquals("marko", vertex.value("name"));
        assertEquals(19L, (long) (Integer) vertex.value("age"));
        Assert.assertFalse(traversal.hasNext());
        assertEquals(6L, FireflyCloseableIteratorUtils.count(this.g.V(new Object[0])));
    }

    static class StubMutationListener implements MutationListener {
        private final AtomicLong addEdgeEvent = new AtomicLong(0);
        private final AtomicLong addVertexEvent = new AtomicLong(0);
        private final AtomicLong vertexRemovedEvent = new AtomicLong(0);
        private final AtomicLong edgePropertyChangedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyChangedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyPropertyChangedEvent = new AtomicLong(0);
        private final AtomicLong edgePropertyRemovedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyPropertyRemovedEvent = new AtomicLong(0);
        private final AtomicLong edgeRemovedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyRemovedEvent = new AtomicLong(0);

        private final ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();

        public void reset() {
            addEdgeEvent.set(0);
            addVertexEvent.set(0);
            vertexRemovedEvent.set(0);
            edgePropertyChangedEvent.set(0);
            vertexPropertyChangedEvent.set(0);
            vertexPropertyPropertyChangedEvent.set(0);
            vertexPropertyPropertyRemovedEvent.set(0);
            edgePropertyRemovedEvent.set(0);
            edgeRemovedEvent.set(0);
            vertexPropertyRemovedEvent.set(0);

            order.clear();
        }

        public List<String> getOrder() {
            return new ArrayList<>(this.order);
        }

        @Override
        public void vertexAdded(final Vertex vertex) {
            addVertexEvent.incrementAndGet();
            order.add("v-added-" + vertex.id());
        }

        @Override
        public void vertexRemoved(final Vertex vertex) {
            vertexRemovedEvent.incrementAndGet();
            order.add("v-removed-" + vertex.id());
        }

        @Override
        public void edgeAdded(final Edge edge) {
            addEdgeEvent.incrementAndGet();
            order.add("e-added-" + edge.id());
        }

        @Override
        public void edgePropertyRemoved(final Edge element, final Property o) {
            edgePropertyRemovedEvent.incrementAndGet();
            order.add("e-property-removed-" + element.id() + "-" + o);
        }

        @Override
        public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property o) {
            vertexPropertyPropertyRemovedEvent.incrementAndGet();
            order.add("vp-property-removed-" + element.id() + "-" + o);
        }

        @Override
        public void edgeRemoved(final Edge edge) {
            edgeRemovedEvent.incrementAndGet();
            order.add("e-removed-" + edge.id());
        }

        @Override
        public void vertexPropertyRemoved(final VertexProperty vertexProperty) {
            vertexPropertyRemovedEvent.incrementAndGet();
            order.add("vp-property-removed-" + vertexProperty.id());
        }

        @Override
        public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {
            edgePropertyChangedEvent.incrementAndGet();
            order.add("e-property-chanaged-" + element.id());
        }

        @Override
        public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {
            vertexPropertyPropertyChangedEvent.incrementAndGet();
            order.add("vp-property-changed-" + element.id());
        }

        @Override
        public void vertexPropertyChanged(final Vertex element, final VertexProperty oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
            vertexPropertyChangedEvent.incrementAndGet();
            order.add("v-property-changed-" + element.id());
        }

        public long addEdgeEventRecorded() {
            return addEdgeEvent.get();
        }

        public long addVertexEventRecorded() {
            return addVertexEvent.get();
        }

        public long vertexRemovedEventRecorded() {
            return vertexRemovedEvent.get();
        }

        public long edgeRemovedEventRecorded() {
            return edgeRemovedEvent.get();
        }

        public long edgePropertyRemovedEventRecorded() {
            return edgePropertyRemovedEvent.get();
        }

        public long vertexPropertyRemovedEventRecorded() {
            return vertexPropertyRemovedEvent.get();
        }

        public long vertexPropertyPropertyRemovedEventRecorded() {
            return vertexPropertyPropertyRemovedEvent.get();
        }

        public long edgePropertyChangedEventRecorded() {
            return edgePropertyChangedEvent.get();
        }

        public long vertexPropertyChangedEventRecorded() {
            return vertexPropertyChangedEvent.get();
        }

        public long vertexPropertyPropertyChangedEventRecorded() {
            return vertexPropertyPropertyChangedEvent.get();
        }
    }

    public void tryCommit(final Graph graph, final Consumer<Graph> assertFunction) {
        assertFunction.accept(graph);
        if (graph.features().graph().supportsTransactions()) {
            graph.tx().commit();
            assertFunction.accept(graph);
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldTriggerAddVertexWithPropertyThenPropertyAdded() {
        StubMutationListener listener1 = new StubMutationListener();
        StubMutationListener listener2 = new StubMutationListener();
        EventStrategy.Builder builder = EventStrategy.build().addListener(listener1).addListener(listener2);
        if (graph.features().graph().supportsTransactions()) {
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));
        }

        EventStrategy eventStrategy = builder.create();
        Vertex vSome = graph.addVertex("some", "thing");
        vSome.property(VertexProperty.Cardinality.single, "that", "thing");
        GraphTraversalSource gts = create(eventStrategy);
        gts.V().addV().property("any", "thing").property(VertexProperty.Cardinality.single, "this", "thing").next();
        tryCommit(graph, (g) -> {
            long val = FireflyCloseableIteratorUtils.count(gts.V(new Object[0]).has("this", "thing"));
            assertEquals(1L, val);
        });
        assertEquals(1L, listener1.addVertexEventRecorded());
        assertEquals(1L, listener2.addVertexEventRecorded());
        assertEquals(1L, listener2.vertexPropertyChangedEventRecorded());
        assertEquals(1L, listener1.vertexPropertyChangedEventRecorded());
    }

    @Test
    public void shouldTriggerUpdateEdgePropertyAddedViaMergeE() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex v = graph.addVertex();
        v.addEdge("self", v);

        final GraphTraversalSource gts = create(eventStrategy);
        final Map<Object, Object> m = new HashMap<>();
        m.put(T.label, "self");
        final Map<Object, Object> mMatch = new HashMap<>();
        mMatch.put("some", "thing");
        gts.V(v).mergeE(m).option(onMatch, mMatch).next();

        tryCommit(graph, g -> assertEquals(1, FireflyCloseableIteratorUtils.count(gts.E().has("some", "thing"))));

        assertEquals(1, FireflyCloseableIteratorUtils.count(gts.E()));

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        assertEquals(0, listener1.addEdgeEventRecorded());
        assertEquals(0, listener2.addEdgeEventRecorded());

        assertEquals(1, listener2.edgePropertyChangedEventRecorded());
        assertEquals(1, listener1.edgePropertyChangedEventRecorded());
    }

    @Test
    public void shouldTriggerEdgePropertyChanged() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("self", v);
        e.property("some", "thing");

        final GraphTraversalSource gts = create(eventStrategy);
        gts.E(e).property("some", "other thing").next();

        tryCommit(graph, g -> assertEquals(1, FireflyCloseableIteratorUtils.count(gts.E().has("some", "other thing"))));

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        assertEquals(0, listener1.addEdgeEventRecorded());
        assertEquals(0, listener2.addEdgeEventRecorded());

        assertEquals(1, listener2.edgePropertyChangedEventRecorded());
        assertEquals(1, listener1.edgePropertyChangedEventRecorded());
    }

    @Test
    public void shouldTriggerAddEdgePropertyAdded() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex v = graph.addVertex();
        v.addEdge("self", v);

        final GraphTraversalSource gts = create(eventStrategy);
        gts.V(v).as("v").addE("self").to("v").property("some", "thing").next();

        tryCommit(graph, g -> assertEquals(1, FireflyCloseableIteratorUtils.count(gts.E().has("some", "thing"))));

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        assertEquals(1, listener1.addEdgeEventRecorded());
        assertEquals(1, listener2.addEdgeEventRecorded());

        assertEquals(0, listener2.edgePropertyChangedEventRecorded());
        assertEquals(0, listener1.edgePropertyChangedEventRecorded());
    }

    @Test
    public void g_V_hasXname_gtXmX_andXcontainingXoXXX() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        Traversal<Vertex, Vertex> traversal = g.V().has("name", P.gt("m").and(TextP.containing("o")));
        this.printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertTrue(((Vertex) traversal.next()).value("name").equals("marko"));
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_E_hasLabelXknowsX() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        Traversal<Edge, Edge> traversal = this.g.E().hasLabel("knows");
        this.printTraversalForm(traversal);
        int counter = 0;

        while (traversal.hasNext()) {
            ++counter;
            assertEquals("knows", ((Edge) traversal.next()).label());
        }

        assertEquals(2L, (long) counter);
    }

    private static <A, B> boolean internalCheckMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());
        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());

        if (expectedList.size() != actualList.size()) {
            return false;
        }

        for (int i = 0; i < actualList.size(); i++) {
            if (!Objects.equals(actualList.get(i).getKey(), expectedList.get(i).getKey())) {
                return false;
            }
            if (!Objects.equals(actualList.get(i).getValue(), expectedList.get(i).getValue())) {
                return false;
            }
        }
        return true;
    }

    private static <A> boolean internalCheckList(final List<A> expectedList, final List<A> actualList) {
        if (expectedList.size() != actualList.size()) {
            return false;
        }
        for (int i = 0; i < actualList.size(); i++) {
            if (!actualList.get(i).equals(expectedList.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertThat(traversal.hasNext(), Is.is(false));
        if (expectedResults.size() != results.size()) {
            assertEquals("Checking result size", expectedResults.size(), results.size());
        }

        for (T t : results) {
            if (t instanceof Map) {
                assertThat("Checking map result existence: " + t, expectedResults.stream().filter(e -> e instanceof Map).anyMatch(e -> internalCheckMap((Map) e, (Map) t)), Is.is(true));
            } else if (t instanceof List) {
                assertThat("Checking list result existence: " + t, expectedResults.stream().filter(e -> e instanceof List).anyMatch(e -> internalCheckList((List) e, (List) t)), Is.is(true));
            } else {
                assertThat("Checking result existence: " + t, expectedResults.contains(t), Is.is(true));
            }
        }
        final Map<T, Long> expectedResultsCount = new HashMap<>();
        final Map<T, Long> resultsCount = new HashMap<>();
        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1L));
        results.forEach(t -> MapHelper.incr(resultsCount, t, 1L));
        assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
        expectedResultsCount.forEach((k, v) -> assertEquals("Checking result group counts", v, resultsCount.get(k)));
    }

    @Test
    public void g_V_branchXageX_optionXltX30X__youngX_optionXgtX30X__oldX_optionXnone__on_the_edgeX() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        Traversal<Vertex, Object> traversal = g.V().hasLabel("person")
                .branch(__.values("age"))
                .option(lt(30), constant("young"))
                .option(gt(30), constant("old"))
                .option(none, constant("on the edge"));
        this.printTraversalForm(traversal);
        checkResults(Arrays.asList("young", "young", "old", "old"), traversal);
    }

    @Test
    public void testTrivalMerge() {
        if (graph.features().vertex().supportsMultiProperties()) {
            GraphTraversalSource g = graph.traversal();
            g.mergeV(new HashMap<>() {{
                put("name", "Brandy");
            }}).next();
            assertTrue(g.V().has("name", "Brandy").hasNext());
            g.mergeV(new HashMap<>() {{
                put(T.label, "Dog");
                put("name", "Scamp");
                put("age", 12);
            }}).next();
            Map<Object, Object> x = g.V().hasLabel("Dog").valueMap().next();
            List<Object> a = (List<Object>) x.get("name");
            List<Object> b = (List<Object>) x.get("age");
            assertEquals("Scamp", a.get(0));
            assertEquals(12, b.get(0));
            g.mergeV(new HashMap<>() {{
                put(T.id, 300);
                put(T.label, "Dog");
                put("name", "Toby");
                put("age", 10);
            }}).next();
            Long y = g.V().hasLabel("Dog").valueMap().with(WithOptions.tokens).count().next();
            assertEquals((Long) 2L, y);
            List<Map<Object, Object>> list = g.V().hasLabel("Dog").valueMap().with(WithOptions.tokens).toList();
            assertEquals(1, list.stream().filter(it -> {
                return ((List<Object>) it.get("age")).get(0).equals(10);
            }).collect(Collectors.toList()).size());
            assertEquals(1, list.stream().filter(it -> {
                return ((List<Object>) it.get("age")).get(0).equals(12);
            }).collect(Collectors.toList()).size());
        } else {
            LOG.info("Skipping testTrivialMerge {} does not support multi-properties", graph);
        }
    }

    @Test
    public void TestMergeEvent() {
        if (graph.features().vertex().supportsMultiProperties()) {
            AtomicReference<String> val = new AtomicReference<>("");
            MutationListener l = new MutationListener() {
                @Override
                public void vertexAdded(Vertex vertex) {
                    val.set("vertexAdded");
                }

                @Override
                public void vertexRemoved(Vertex vertex) {
                    val.set("vertexRemoved");
                }

                @Override
                public void vertexPropertyChanged(Vertex element, VertexProperty oldValue, Object setValue, Object... vertexPropertyKeyValues) {
                    val.set("vertexPropertyChanged");
                }

                @Override
                public void vertexPropertyRemoved(VertexProperty vertexProperty) {
                    val.set("vertexPropertyRemoved");
                }

                @Override
                public void edgeAdded(Edge edge) {
                    val.set("edgeAdded");
                }

                @Override
                public void edgeRemoved(Edge edge) {
                    val.set("edgeRemoved");
                }

                @Override
                public void edgePropertyChanged(Edge element, Property oldValue, Object setValue) {
                    val.set("edgePropertyChanged");
                }

                @Override
                public void edgePropertyRemoved(Edge element, Property property) {
                    val.set("edgePropertyRemoved");
                }

                @Override
                public void vertexPropertyPropertyChanged(VertexProperty element, Property oldValue, Object setValue) {
                    val.set("vertexPropertyPropertyChanged");
                }

                @Override
                public void vertexPropertyPropertyRemoved(VertexProperty element, Property property) {
                    val.set("vertexPropertyPropertyRemoved");
                }
            };
            EventStrategy strategy = EventStrategy.build().addListener(l).create();
            g = graph.traversal().withStrategies(strategy);
            AtomicBoolean b = new AtomicBoolean(false);
            try {
                g.mergeV(new HashMap<>() {{
                            put(T.id, 1);
                        }})
                        .option(onCreate, __.fail("vertex did not exist"))
                        .option(onMatch, new HashMap<>() {{
                            put("modified", 2022);
                        }}).next();
            } catch (FailStep.FailException fe) {
                b.set(true);
            }
            assertTrue(b.get());
            assertEquals("", val.get());
        } else {
            LOG.info("skipping TestMergeEvent {} does not support multi properties", graph);
        }
    }

    @Test
    public void g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
        if (graph.features().vertex().supportsMultiProperties()) {
            Graph tg = TinkerFactory.createTheCrew();
            GraphHelper.cloneElements(tg, graph);
            Traversal<Vertex, String> traversal = g.V().local(properties("location").order().by(T.value, Order.asc).range(0, 2)).value();
            this.printTraversalForm(traversal);
            checkResults(Arrays.asList("brussels", "san diego", "centreville", "dulles", "baltimore", "bremen", "aachen", "kaiserslautern"), traversal);
        } else {
            LOG.info("skipping g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value because {} does not support multi-properties", graph);
        }
    }

    @Test
    public void g_mergeVXlabel_person_name_markoX() {
        Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        final Traversal<Vertex, Vertex> traversal = g.mergeV(asMap(T.label, "person", "name", "marko"));
        final Vertex vertex = traversal.next();
        assertEquals("person", vertex.label());
        assertEquals("marko", vertex.<String>value("name"));
        Assert.assertFalse(traversal.hasNext());
        assertEquals(6, FireflyCloseableIteratorUtils.count(g.V()));
    }

    @Test
    public void g_V_localXoutE_countX() {
        Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
        Traversal<Vertex, Long> traversal = g.V().local(outE().count());
        this.printTraversalForm(traversal);
        checkResults(Arrays.asList(3L, 0L, 0L, 0L, 1L, 2L), traversal);
    }

    @Test
    public void g_V_localXoutE_countX_uncached() {
        Configuration nocacheconfig = ConfigurationUtils.cloneConfiguration(config);
        nocacheconfig.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "true");

        nocacheconfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        nocacheconfig.setProperty(Graph.GRAPH, "ncg");

        FireflyGraph noCacheGraph = FireflyGraph.open(nocacheconfig);
        noCacheGraph.getBaseGraph().dropDatabase();

        GraphHelper.cloneElements(TinkerFactory.createModern(), noCacheGraph);
        Traversal<Vertex, Long> traversal = noCacheGraph.traversal().V().local(outE().count());
        this.printTraversalForm(traversal);

        LOG.info(noCacheGraph.traversal().V().local(outE()).toList().toString());
        LOG.info(noCacheGraph.traversal().V().local(outE().count()).toList().toString());
        LOG.info(String.valueOf(noCacheGraph.traversal().V().outE().count().next()));
        noCacheGraph.traversal().V().forEachRemaining(v -> {
            LOG.info(v.id().toString());
            LOG.info(String.valueOf(noCacheGraph.traversal().V(v).outE().count().next()));
        });

        checkResults(Arrays.asList(3L, 0L, 0L, 0L, 1L, 2L), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_optionalXinXknowsXX() {
        Configuration nocacheconfig = ConfigurationUtils.cloneConfiguration(config);
        nocacheconfig.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "true");

        nocacheconfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        nocacheconfig.setProperty(Graph.GRAPH, "ncg");

        FireflyGraph noCacheGraph = FireflyGraph.open(nocacheconfig);
        noCacheGraph.getBaseGraph().dropDatabase();

        GraphHelper.cloneElements(TinkerFactory.createModern(), noCacheGraph);


        Vertex fv = noCacheGraph.traversal().V().has("name", "vadas").next();
        Vertex rv = TinkerFactory.createModern().traversal().V().has("name", "vadas").next();
        assertEquals(rv, fv);
        var fvOutList = noCacheGraph.traversal().V().has("name", "vadas").out().toList();
        var rvOutList = TinkerFactory.createModern().traversal().V().has("name", "vadas").out().toList();

        var fvInVList = noCacheGraph.traversal().V().has("name", "vadas").in().toList();
        var rvInVList = TinkerFactory.createModern().traversal().V().has("name", "vadas").in().toList();

        var fvInEList = noCacheGraph.traversal().V().has("name", "vadas").inE().toList();
        var rvInEList = TinkerFactory.createModern().traversal().V().has("name", "vadas").inE().toList();

        final Traversal<Vertex, Vertex> traversal = noCacheGraph.traversal().V(convertToVertexId(noCacheGraph, "vadas")).optional(in("knows"));
        Set<Vertex> nocacheResult = noCacheGraph.traversal().V(convertToVertexId(noCacheGraph, "vadas")).in("knows").toSet();
        Set<Vertex> refrence = TinkerFactory.createModern().traversal().V(convertToVertexId(noCacheGraph, "vadas")).in("knows").toSet();
        assertEquals(refrence, nocacheResult);

        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(convertToVertex(noCacheGraph, "marko"), traversal.next());
    }

    @Test
    public void g_V_both_both_dedup_byXlabelX() {
        Configuration nocacheconfig = ConfigurationUtils.cloneConfiguration(config);
        nocacheconfig.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "true");

        nocacheconfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        nocacheconfig.setProperty(Graph.GRAPH, "ncg");

        FireflyGraph noCacheGraph = FireflyGraph.open(nocacheconfig);
        noCacheGraph.getBaseGraph().dropDatabase();

        GraphHelper.cloneElements(TinkerFactory.createModern(), noCacheGraph);

        Vertex marko = noCacheGraph.traversal().V().has("name", "marko").next();
        List<Vertex> resultList = noCacheGraph.traversal().V(marko).both().toList();
        List<Vertex> referenceList = TinkerFactory.createModern().traversal().V(marko).both().toList();

        Collections.sort(referenceList, Comparator.comparingInt(Object::hashCode));
        Collections.sort(resultList, Comparator.comparingInt(Object::hashCode));

        LOG.info(String.format("firefly result: %s", resultList));
        LOG.info(String.format("reference result: %s", referenceList));


        try {
            assertEquals(referenceList, resultList);
        } catch (AssertionError e) {
            LOG.info("--------------------------");
            resultList.forEach(v -> {
                LOG.info("===============RESULT================");
                LOG.info((String) v.id());
                LOG.info(FireflyCloseableIteratorUtils.list(v.properties()).toString());
                LOG.info(v.label());
                noCacheGraph.traversal().V(v).dedup().bothE()
                        .toStream()
                        .sorted(Comparator.comparingInt(Object::hashCode))
                        .forEach(System.out::println);
                LOG.info("=====================================");
            });

            referenceList.forEach(v -> {
                LOG.info("===============REF==================");
                LOG.info((String) v.id());
                LOG.info(FireflyCloseableIteratorUtils.list(v.properties()).toString());
                LOG.info(v.label());
                noCacheGraph.traversal().V(v).dedup().bothE()
                        .toStream()
                        .sorted(Comparator.comparingInt(Object::hashCode))
                        .forEach(System.out::println);
                LOG.info("=====================================");
            });

            throw e;
        }


        Traversal<Vertex, Vertex> traversal = noCacheGraph.traversal().V().both().both().dedup().by(T.label);
        this.printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        assertEquals(2L, (long) vertices.size());
    }

    @Test
    public void g_V_hasLabelXloopsX_bothXselfX() {
        Configuration nocacheconfig = ConfigurationUtils.cloneConfiguration(config);
        nocacheconfig.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "true");

        nocacheconfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        nocacheconfig.setProperty(Graph.GRAPH, "ncg");

        FireflyGraph noCacheGraph = FireflyGraph.open(nocacheconfig);
        noCacheGraph.getBaseGraph().dropDatabase();

        GraphHelper.cloneElements(TinkerFactory.createKitchenSink(), noCacheGraph);


        Traversal<Vertex, Vertex> traversal = noCacheGraph.traversal().V().hasLabel("loops").both("self");
        this.printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        assertEquals(2L, (long) vertices.size());
        assertEquals(vertices.get(0), vertices.get(1));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_valueMap() {
        Configuration nocacheconfig = ConfigurationUtils.cloneConfiguration(config);
        nocacheconfig.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "true");

        nocacheconfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        nocacheconfig.setProperty(Graph.GRAPH, "ncg");

        FireflyGraph noCacheGraph = FireflyGraph.open(nocacheconfig);
        noCacheGraph.getBaseGraph().dropDatabase();

        Configuration nostrategyconfig = ConfigurationUtils.cloneConfiguration(config);
//        nostrategyconfig.setProperty(ENABLE_COMPOSITE_ID_STRATEGY.toLowerCase(), "false");
        nostrategyconfig.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "true");
        nostrategyconfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "nsg");
        nostrategyconfig.setProperty(Graph.GRAPH, "nsg");

        FireflyGraph noStrategyGraph = FireflyGraph.open(nostrategyconfig);
        noStrategyGraph.getBaseGraph().dropDatabase();

        GraphHelper.cloneElements(TinkerFactory.createModern(), noCacheGraph);
        GraphHelper.cloneElements(TinkerFactory.createModern(), noStrategyGraph);
        final Traversal<Vertex, Map<Object, List<String>>> traversal = noStrategyGraph.traversal().V(convertToVertexId(noStrategyGraph, "marko")).out("created").valueMap();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        List<Map<Object, Object>> totalResults = noStrategyGraph.traversal().V(convertToVertexId(noStrategyGraph, "marko")).out("created").valueMap().toList();


        final Map<Object, List<String>> values = traversal.next();
        Map<Object, List<String>> extraValues;
        if (traversal.hasNext()) {
            extraValues = traversal.next();
            LOG.info(extraValues.toString());
        }
        assertEquals("lop", values.get("name").get(0));
        assertEquals("java", values.get("lang").get(0));
        assertEquals(2, values.size());

        final Traversal<Vertex, Map<Object, List<String>>> nctraversal = noCacheGraph.traversal().V(convertToVertexId(noCacheGraph, "marko")).out("created").valueMap();
        printTraversalForm(nctraversal);
        assertTrue(nctraversal.hasNext());
        final Map<Object, List<String>> ncvalues = nctraversal.next();
        Assert.assertFalse(nctraversal.hasNext());
        assertEquals("lop", ncvalues.get("name").get(0));
        assertEquals("java", ncvalues.get("lang").get(0));
        assertEquals(2, ncvalues.size());
    }

    @Test
    public void g_V_hasLabelXloopsX_bothEXselfX() {
        Configuration nocacheconfig = ConfigurationUtils.cloneConfiguration(config);
        nocacheconfig.setProperty(EDGE_CACHE_DISABLED_GLOBALLY.toLowerCase(), "true");

        nocacheconfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        nocacheconfig.setProperty(Graph.GRAPH, "ncg");

        FireflyGraph noCacheGraph = FireflyGraph.open(nocacheconfig);
        noCacheGraph.getBaseGraph().dropDatabase();

        GraphHelper.cloneElements(TinkerFactory.createKitchenSink(), noCacheGraph);
        GraphTraversalSource g = noCacheGraph.traversal();
        Traversal<Vertex, Edge> traversal = g.V(new Object[0]).hasLabel("loops", new String[0]).bothE(new String[]{"self"});
        Traversal<Vertex, Edge> traversalIn = g.V(new Object[0]).hasLabel("loops", new String[0]).inE(new String[]{"self"});
        Traversal<Vertex, Edge> traversalOut = g.V(new Object[0]).hasLabel("loops", new String[0]).outE(new String[]{"self"});

        this.printTraversalForm(traversal);
        List<Vertex> allV = g.V().toList();
        List<Edge> allE = g.E().toList();
        List<Edge> bothEdges = traversal.toList();
        List<Edge> inEdges = traversalIn.toList();
        List<Edge> outEdges = traversalOut.toList();
        assertEquals(2L, (long) bothEdges.size());
        assertEquals(bothEdges.get(0), bothEdges.get(1));
    }

    @Test
    public void g_io_writeXjsonX() throws IOException {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        String fileToWrite = TestHelper.generateTempFile(WriteTest.class, "tinkerpop-modern-v3d0", ".json").getAbsolutePath().replace('\\', '/');
        File f = new File(fileToWrite);
        MatcherAssert.assertThat(f.length() == 0L, Is.is(true));
        Traversal<Object, Object> traversal = this.g.io(fileToWrite).write();
        this.printTraversalForm(traversal);
        traversal.iterate();
        MatcherAssert.assertThat(f.length() > 0L, Is.is(true));
    }

    public Object convertToEdgeId(final String outVertexName, String edgeLabel, final String inVertexName) {
        return this.convertToEdgeId(graph, outVertexName, edgeLabel, inVertexName);
    }

    public Object convertToEdgeId(final Graph graph, final String outVertexName, String edgeLabel, final String inVertexName) {
        return this.convertToEdge(graph, outVertexName, edgeLabel, inVertexName).id();
    }

    @Test
    public void g_EX11X() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        Vertex josh = graph.traversal().V().has("name", "josh").next();
        Edge aJoshOutE = graph.traversal().V(josh).outE("created").next();
        List<Vertex> joshOutEInV = graph.traversal().E(aJoshOutE).inV().toList();
        Object edgeId = graph.traversal().V(josh).outE("created").as("e").inV().has("name", "lop").<Edge>select("e").toList().get(0);
    }

    @Test
    public void g_V_hasXage_withoutX27X_count() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        Traversal<Vertex, Long> traversal = g.V().has("age", P.without(27)).count();
        this.printTraversalForm(traversal);
        assertEquals((Long) 3L, (Long) traversal.next());
    }

    @Test
    public void g_V_hasIdXemptyX_count() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        Traversal<Vertex, Long> traversal = g.V().hasId(Collections.emptyList()).count();
        this.printTraversalForm(traversal);
        assertEquals((Long) 0L, (Long) traversal.next());
    }
}
