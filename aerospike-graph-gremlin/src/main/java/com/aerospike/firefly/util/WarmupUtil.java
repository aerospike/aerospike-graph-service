package com.aerospike.firefly.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class WarmupUtil {
    private static final Logger LOG = LoggerFactory.getLogger(WarmupUtil.class);
    public static final int passes = 16;
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
        // Allow warmup to be disabled.
        if (!ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUTO_PRE_HEAT, conf)) {
            return;
        }

        try {
            if (graph == null) {
                final Configuration warmupConfig = ConfigurationUtils.cloneConfiguration(conf);
                warmupConfig.setProperty(ConfigurationHelper.Keys.WARMUP_MODE.toLowerCase(), "true");
                graph = FireflyGraph.open(warmupConfig);
            }
            IntStream.range(0, passes).forEach(i -> {
                phase1();
                phase2();
            });
        } catch (final Throwable e) {
            ConfigurationHelper.restoreLogLevel(conf);
            LOG.error("Error during warmup: {}", e.getMessage());
        } finally {
            if (graph != null) {
                graph.close();
                graph = null;
            } else {
                // Need to restore log level manually if graph.close() cannot be invoked.
                ConfigurationHelper.restoreLogLevel(conf);
            }
        }
    }

    private void phase1() {
        final GraphTraversalSource g = graph.traversal();
        final List<Object> createdIds = cloneElements(TinkerFactory.createModern(), graph);
        final Object[] createdIdAry = createdIds.toArray(new Object[0]);
        if (ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.LOG_WARMUP_SETS, conf)) {
            System.out.println("Warmup Vertex Set: " + graph.getBaseGraph().VERTEX_AERO_SET);
            System.out.println("Warmup Edge Set: " + graph.getBaseGraph().EDGE_AERO_SET);
        }

        g.V(createdIdAry).has("name", "CANT COME DOWN").outE().inV().count().iterate();

        g.V(createdIdAry).match(
                        __.as("a").has("name", "Garcia"),
                        __.as("a").in("writtenBy").as("b"),
                        __.as("a").in("sungBy").as("b")).
                select("b").values("name").toList();

        g.V(createdIdAry).drop().iterate();
    }

    private void phase2() {
        final GraphTraversalSource g = graph.traversal();
        final List<Object> createdIds = cloneElements(TinkerFactory.createModern(), graph);
        final Object[] createdIdAry = createdIds.toArray(new Object[0]);

        g.V(createdIdAry).hasLabel("person").as("p1").choose(outE("knows"), out("knows")).as("p2").<String>select("p1", "p2").by("name").toList();
        g.V(createdIdAry).hasLabel("person").choose(values("age")).coin(.4).groupCount().next();
        final Vertex a = g.V(createdIdAry).has("name", "marko").next();
        final Vertex b = g.V(createdIdAry).has("name", "peter").next();
        g.withSideEffect("b", b).V(a).addE("knows").to("b").property("weight", 0.5d).toList();
        g.withSideEffect("sg", () -> TinkerGraph.open()).V(createdIdAry).has("name", "marko").outE("knows").subgraph("sg").values("name").cap("sg").toList();
        g.V(createdIdAry).as("a").out().as("b").out().as("c").simplePath().by(T.label).from("b").to("c").path().by("name").toList();

        g.V(createdIdAry).drop().iterate();
    }

    private static List<Object> cloneElements(final Graph original, final Graph clone) {
        final HashMap<Object, Object> vxidmap = new HashMap<>();
        original.vertices().forEachRemaining((origVertex) -> {
            // need to assign new id to avoid conflict where multiple AGS instances running warmup.
            // T.id is unsupported id type, so Firefly will create new one.
            final Vertex newVertex = new DetachedVertex(T.id, origVertex.label(), IteratorUtils.toList(origVertex.properties()))
                    .attach(Attachable.Method.create(clone));

            vxidmap.put(origVertex.id(), newVertex.id());
        });
        original.edges().forEachRemaining((e) -> {
            final Vertex iv = e.inVertex();
            final Vertex ov = e.outVertex();
            final GraphTraversalSource cg = clone.traversal();
            final Edge addedE = clone.traversal().V(vxidmap.get(ov.id())).addE(e.label()).to(__.V(vxidmap.get(iv.id()))).next();
            e.properties().forEachRemaining(p -> {
                cg.E(addedE).property(p.key(), p.value()).iterate();
            });

        });
        return new ArrayList(vxidmap.values());
    }
}
