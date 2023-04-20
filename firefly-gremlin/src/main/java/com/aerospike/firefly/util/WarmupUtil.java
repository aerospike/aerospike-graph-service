package com.aerospike.firefly.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.io.AerospikeConnection;
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
import java.util.Date;
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
    final AerospikeConnection db;
    final FireflyGraph graph;
    public static final int passes = 48;
    final Logger LOG = LoggerFactory.getLogger(WarmupUtil.class);
    private final Configuration conf;

    private WarmupUtil(Configuration conf) {
        this.conf = conf;
        Configuration warmupConfig = ConfigurationUtils.cloneConfiguration(conf);
        String warmupArena = getWarmupArenaName();
        warmupConfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), warmupArena);
        warmupConfig.setProperty(ConfigurationHelper.Keys.WARMUP_MODE.toLowerCase(), "true");
        warmupConfig.setProperty(ConfigurationHelper.Keys.LOG_LEVEL.toLowerCase(), "OFF");
        db = AerospikeConnection.connect(warmupConfig);
        graph = FireflyGraph.open(warmupConfig);
    }

    public static WarmupUtil create(Configuration conf) {
        return new WarmupUtil(conf);
    }

    public static String getWarmupArenaName() {
        return "FIREFLYWARMUP";
    }

    public void preheat(int passes) {
        if (Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.FAULT_TEST, conf))) {
            final String message = "Fault test is enabled by configuration, will stall warmup routine.";
            System.out.println(message);
            LOG.warn(message);
            throw new AerospikeException("Fault Test. The FAULT_TEST configuration key has been enabled. This intentionally causes the warmup routine to fail.");
        }
        LOG.debug("Performing automatic warmup");
        IntStream.range(0, passes).forEach(i -> {
            phase1();
            phase2();
        });
    }

    public void phase1() {
        synchronized (FireflyGraph.class) {
            GraphTraversalSource g = graph.traversal();
            List<Object> createdIds = cloneElements(TinkerFactory.createModern(), graph);
            Object[] createdIdAry = createdIds.toArray(new Object[0]);
            try {
                g.V(createdIdAry).has("name", "CANT COME DOWN").outE().inV().count().next();

                g.V(createdIdAry).match(
                                __.as("a").has("name", "Garcia"),
                                __.as("a").in("writtenBy").as("b"),
                                __.as("a").in("sungBy").as("b")).
                        select("b").values("name").toList();
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            g.V(createdIdAry).drop().iterate();
        }
    }

    public void phase2() {
        synchronized (FireflyGraph.class) {
            GraphTraversalSource g = graph.traversal();
            List<Object> createdIds = cloneElements(TinkerFactory.createModern(), graph);
            Object[] createdIdAry = createdIds.toArray(new Object[0]);
            try {
                g.V(createdIdAry).hasLabel("person").as("p1").choose(outE("knows"), out("knows")).as("p2").<String>select("p1", "p2").by("name").toList();
                g.V(createdIdAry).hasLabel("person").choose(values("age")).coin(.4).groupCount().next();
                final Vertex a = g.V(createdIdAry).has("name", "marko").next();
                final Vertex b = g.V(createdIdAry).has("name", "peter").next();
                g.withSideEffect("b", b).V(a).addE("knows").to("b").property("weight", 0.5d).toList();
                g.withSideEffect("sg", () -> TinkerGraph.open()).V(createdIdAry).has("name", "marko").outE("knows").subgraph("sg").values("name").cap("sg").toList();
                g.V(createdIdAry).as("a").out().as("b").out().as("c").simplePath().by(T.label).from("b").to("c").path().by("name").toList();
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
            g.V(createdIdAry).drop().iterate();

        }
    }


    public static List<Object> cloneElements(final Graph original, final Graph clone) {
        HashMap<Object, Object> vxidmap = new HashMap<>();
        original.vertices(new Object[0]).forEachRemaining((origVertex) -> {
            Vertex newVertex = (Vertex) DetachedFactory.detach(origVertex, true).attach(Attachable.Method.create(clone));
            vxidmap.put(origVertex.id(), newVertex.id());
        });


        original.edges(new Object[0]).forEachRemaining((e) -> {
            Vertex iv = e.inVertex();
            Vertex ov = e.outVertex();
            GraphTraversalSource cg = clone.traversal();
            Edge addedE = clone.traversal().V(vxidmap.get(ov.id())).addE(e.label()).to(__.V(vxidmap.get(iv.id()))).next();
            e.properties().forEachRemaining(p -> {
                cg.E(addedE).property(p.key(), p.value());
            });

        });
        return Arrays.asList(vxidmap.values().toArray());
    }

    public static void invokeWarmup(GraphTraversalSource g) {
        Logger LOG = LoggerFactory.getLogger(WarmupUtil.class);
        System.out.println("Will attempt warmup routine");
        try {
            IntStream.range(0, 48).forEach(it -> {
                System.out.printf("Warmup routine iteration %d at time %s \n", it, new Date());
                g.V("FIREFLY_WARMUP").next();
            });
        } catch (Exception e) {
            final String message = String.format("Failed to perform warmup routine: %s", e.getMessage());
            LOG.error(message);
            System.err.println(message);
        }
    }
}
