package com.aerospike.firefly.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import static com.aerospike.firefly.structure.FireflyGraph.FIREFLY_WARMUP_VARIABLE_NAME;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class WarmupUtil {
    public static final int passes = 48;
    private static final Logger LOG = LoggerFactory.getLogger(WarmupUtil.class);
    private final Configuration conf;
    private static FireflyGraph graph = null;

    private WarmupUtil(final Configuration conf) {
        this.conf = conf;
    }

    public static WarmupUtil create(final Configuration conf) {
        return new WarmupUtil(conf);
    }

    public static String getWarmupArenaName() {
        return "FIREFLYWARMUP";
    }

    public void preheat(final int passes) {
        synchronized (FireflyGraph.class) {
            if (ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.FAULT_TEST, conf)) {
                final String message = "Fault Test. The '" + ConfigurationHelper.Keys.FAULT_TEST + "' configuration key has been enabled. This intentionally causes the warmup routine to fail.";
                System.out.println(message);
                throw new AerospikeException(message);
            }
            try {
                if (graph == null) {
                    final Configuration warmupConfig = ConfigurationUtils.cloneConfiguration(conf);
                    final String warmupArena = getWarmupArenaName();
                    warmupConfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), warmupArena);
                    warmupConfig.setProperty(ConfigurationHelper.Keys.WARMUP_MODE.toLowerCase(), "true");
                    warmupConfig.setProperty(ConfigurationHelper.Keys.SUMMARY_ENABLED_FLAG.toLowerCase(), "false");
                    warmupConfig.setProperty(ConfigurationHelper.Keys.SUMMARY_TICKER_ENABLED_FLAG.toLowerCase(), "false");
                    warmupConfig.setProperty(ConfigurationHelper.Keys.LOG_LEVEL.toLowerCase(), "OFF");
                    graph = FireflyGraph.open(warmupConfig);
                }
                IntStream.range(0, passes).forEach(i -> {
                    phase1();
                    phase2();
                });
            } catch (final Exception e) {
                LOG.error("Error during warmup", e);
            } finally {
                if (graph != null) {
                    graph.close();
                    graph = null;
                }
            }
        }
    }

    private void phase1() {
        final GraphTraversalSource g = graph.traversal();
        final List<Object> createdIds = cloneElements(TinkerFactory.createModern(), graph);
        final Object[] createdIdAry = createdIds.toArray(new Object[0]);
        try {
            g.V(createdIdAry).has("name", "CANT COME DOWN").outE().inV().count().next();

            g.V(createdIdAry).match(
                            __.as("a").has("name", "Garcia"),
                            __.as("a").in("writtenBy").as("b"),
                            __.as("a").in("sungBy").as("b")).
                    select("b").values("name").toList();
        } catch (final Exception e) {
            LOG.warn(e.getMessage());
        }
        g.V(createdIdAry).drop().iterate();
    }

    private void phase2() {
        final GraphTraversalSource g = graph.traversal();
        final List<Object> createdIds = cloneElements(TinkerFactory.createModern(), graph);
        final Object[] createdIdAry = createdIds.toArray(new Object[0]);
        try {
            g.V(createdIdAry).hasLabel("person").as("p1").choose(outE("knows"), out("knows")).as("p2").<String>select("p1", "p2").by("name").toList();
            g.V(createdIdAry).hasLabel("person").choose(values("age")).coin(.4).groupCount().next();
            final Vertex a = g.V(createdIdAry).has("name", "marko").next();
            final Vertex b = g.V(createdIdAry).has("name", "peter").next();
            g.withSideEffect("b", b).V(a).addE("knows").to("b").property("weight", 0.5d).toList();
            g.withSideEffect("sg", () -> TinkerGraph.open()).V(createdIdAry).has("name", "marko").outE("knows").subgraph("sg").values("name").cap("sg").toList();
            g.V(createdIdAry).as("a").out().as("b").out().as("c").simplePath().by(T.label).from("b").to("c").path().by("name").toList();
        } catch (final Exception e) {
            LOG.warn(e.getMessage());
        }
        g.V(createdIdAry).drop().iterate();
    }


    private static List<Object> cloneElements(final Graph original, final Graph clone) {
        final HashMap<Object, Object> vxidmap = new HashMap<>();
        original.vertices(new Object[0]).forEachRemaining((origVertex) -> {
            Vertex newVertex = (Vertex) DetachedFactory.detach(origVertex, true).attach(Attachable.Method.create(clone));
            vxidmap.put(origVertex.id(), newVertex.id());
        });
        original.edges(new Object[0]).forEachRemaining((e) -> {
            final Vertex iv = e.inVertex();
            final Vertex ov = e.outVertex();
            final GraphTraversalSource cg = clone.traversal();
            final Edge addedE = clone.traversal().V(vxidmap.get(ov.id())).addE(e.label()).to(__.V(vxidmap.get(iv.id()))).next();
            e.properties().forEachRemaining(p -> {
                cg.E(addedE).property(p.key(), p.value());
            });

        });
        return Arrays.asList(vxidmap.values().toArray());
    }

    public static void invokeWarmup(final GraphTraversalSource g) {
        System.out.println("Will attempt warmup routine");
        try {
            g.V(FIREFLY_WARMUP_VARIABLE_NAME).next();
            System.out.println("Warmup complete.");
        } catch (final Exception e) {
            if (e.getMessage().contains("Failure to initialize security context")) {
                // Silently fail; can't warm up when were secure.
                return;
            }
            final String message = String.format("Failed to perform warmup routine: %s", e.getMessage());
            LOG.error(message);
            System.err.println(message);
        }
    }
}
