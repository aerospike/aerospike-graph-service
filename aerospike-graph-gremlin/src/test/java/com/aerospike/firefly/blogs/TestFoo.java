package com.aerospike.firefly.blogs;

import com.aerospike.firefly.Tokens;
import com.aerospike.firefly.process.traversal.step.util.TaskLogger;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestFoo {
    protected static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
        config.setProperty(ConfigurationHelper.Keys.LOG_LEVEL.toLowerCase(), "info");
        config.setProperty("aerospike.graph.index.vertex.label.enabled", "true");
        config.setProperty("aerospike.graph.index.vertex.properties", "ip,application");
        config.setProperty("aerospike.client.scan.max.wait", "20000");
        config.setProperty("aerospike.client.clientPolicy.timeout", "20000");
        config.setProperty("aerospike.client.policy.read.socketTimeout", "500");
        config.setProperty("aerospike.client.policy.read.totalTimeout", "1500");
        config.setProperty("aerospike.graph.log.supernode.warning", "false");
    }

    static class RunQuery implements Callable {
        private final GraphTraversal traversal;
        public RunQuery(GraphTraversal traversal) {
            this.traversal = traversal;
        }

        @Override
        public List<Path> call() {
            return traversal.toList();
        }
    }

    @Test
    public void q() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal().with("evaluationTimeout", 24 * 60 * 60 * 1000);

            List q = g.V()
                    .hasLabel("VM").as("srcVM")
                    .out("HAS_INTERFACE").hasLabel("Interface").has("ip", "10.194.18.160").as("srcInterface")
                    .out("BELONGS_TO_CIDR").hasLabel("CIDR").as("srcCIDR")
                    .hasId(33784)
//                    .dedup()
                    .outE("SENDS_TRAFFIC").as("sendsTraffic")
                    // hasLabel just for visibility
                    // 48780 is here
                    .inV().hasLabel("Interface").as("fwInterface")
                    .hasId(25813).limit(2)
//                    .dedup()
                    .outE("FORWARDS_TRAFFIC").as("forwardsTraffic")
                    .has("ruleHash", __.select("sendsTraffic").values("ruleHash"))
                    // hasLabel just for visibility
                    .inV().hasLabel("CIDR").as("destCIDR")
                    .in("BELONGS_TO_CIDR").has("ip", "10.1.20.208").as("destVMInterface") // VM 10061

//                    .count() // 534
                    .path()
                    .limit(10)
                    .toList();
            System.out.println(q);
        }
    }

    @Test
    public void qasdfasf() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal().with("evaluationTimeout", 24 * 60 * 60 * 1000);

            List q = g.V()
                    .hasLabel("VM").as("srcVM")
                    .out("HAS_INTERFACE").hasLabel("Interface").has("ip", "10.194.18.160").as("srcInterface")
                    .out("BELONGS_TO_CIDR").hasLabel("CIDR").as("srcCIDR")
                    .outE("SENDS_TRAFFIC").as("sendsTraffic")
                    .inV().hasLabel("Interface").as("fwInterface")
                    .outE("FORWARDS_TRAFFIC").as("forwardsTraffic")
                    .has("ruleHash", __.select("sendsTraffic").values("ruleHash"))
                    .inV().hasLabel("CIDR").as("destCIDR")
                    .in("BELONGS_TO_CIDR").has("ip", "10.1.20.208").as("destVMInterface") // VM 10061
                    .count()
                    .toList();
            System.out.println(q);
        }
    }

    @Test
    public void query3() {
        /// "MATCH (srcVM:VM)-[:HAS_INTERFACE]->(srcInterface:Interface) WHERE srcInterface.ip = $src OR  $src IN split(srcInterface.ip, ',') WITH srcVM, srcInterface
        //OPTIONAL MATCH (srcVM:VM)<-[:HAS_VM]-(srcHv:HyperVisor)<-[:HAS_HYPERVISOR] -(srcRack:Rack)<-[:HAS_RACK]-(srcFloor:Floor)<-[:HAS_FLOOR]-(srcDC:Datacenter)
        //OPTIONAL MATCH (srcInterface:Interface)-[:BELONGS_TO_CIDR]->(srcCIDR:CIDR)-[sendsTraffic:SENDS_TRAFFIC]-(fwInterface:Interface)
        //WITH sendsTraffic, srcVM, srcDC, srcFloor, srcRack, srcHv, srcInterface
        //OPTIONAL MATCH (destCIDR:CIDR)<-[forwardsTraffic:FORWARDS_TRAFFIC]-(fwInterface:Interface) WHERE sendsTraffic.ruleHash = forwardsTraffic.ruleHash
        //OPTIONAL MATCH (fwInterface)<-[:HAS_INTERFACE]-(firewall:Firewall)
        //OPTIONAL MATCH (destVMInterface)-[:BELONGS_TO_CIDR]->(destCIDR)
        //MATCH (destVM:VM)-[:HAS_INTERFACE]->(destVMInterface) WHERE destVMInterface.ip = $dest OR $dest IN split(destVMInterface.ip, ',')
        //OPTIONAL MATCH (destDC:Datacenter)-[:HAS_FLOOR]->(destFloor:Floor)-[:HAS_RACK]->(destRack:Rack)-[:HAS_HYPERVISOR]->(destHv:HyperVisor)-[:HAS_VM]->(destVM)
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal().with("evaluationTimeout", 24 * 60 * 60 * 1000);

            Instant start = Instant.now();

            ExecutorService executorService = Executors.newFixedThreadPool(3);
            Future<List<Path>> output1 = executorService.submit(new RunQuery(g.V().hasLabel("Interface").has("ip", "10.194.18.160").as("srcInterface").
                    out("BELONGS_TO_CIDR").hasLabel("CIDR").as("srcCIDR")
                    .outE("SENDS_TRAFFIC").as("sendsTraffic")
                    .inV().hasLabel("Interface").as("fwInterface").path()));

            Future<List<Path>> output2 = executorService.submit(new RunQuery(g.V().hasLabel("Interface").has("ip", "10.1.20.208").as("destInterface").
                    out("BELONGS_TO_CIDR").as("destCIDR").
                    inE("FORWARDS_TRAFFIC").as("forwardsTraffic").
                    outV().as("fwInterface").path()));

            Future<List<Path>> output3 = executorService.submit(new RunQuery(
                    g.V().hasLabel("Interface").has("ip", "10.1.20.208").as("destInterface").
                    in("HAS_INTERFACE").as("destVM").
                    in("HAS_VM").as("destHv").
                    in("HAS_HYPERVISOR").as("destRack").
                    in("HAS_RACK").as("destFloor").
                    in("HAS_FLOOR").as("destDC").path()));
            //OPTIONAL MATCH (destDC:Datacenter)-[:HAS_FLOOR]->(destFloor:Floor)-[:HAS_RACK]->(destRack:Rack)-[:HAS_HYPERVISOR]->(destHv:HyperVisor)-[:HAS_VM]->(destVM)

            try {
                List<Path> fwInterface1 = output1.get();
                List<Path> fwInterface2 = output2.get();
                List<Path> fwInterface3 = output3.get();
                assert fwInterface3.size() == 1;
                List<Path> intersectedPath = new ArrayList<>();
                System.out.println("fwInterface1: " + fwInterface1.size());
                System.out.println("fwInterface2: " + fwInterface2.size());

                for (final Path fw1 : fwInterface1) {
                    final Object o1 = fw1.objects().get(fw1.objects().size() - 1);
                    for (final Path fw2 : fwInterface2) {
                        final Object o2 = fw2.objects().get(fw2.objects().size() - 1);
                        if (o1.equals(o2)) {
                            final Path mutablePath = MutablePath.make();
                            final List<Object> objects = fw1.objects();
                            final List<Set<String>> labels = fw1.labels();
                            for (int i = 0; i < objects.size(); i++) {
                                mutablePath.extend(objects.get(i), labels.get(i));
                            }
                            for (int i = fw2.objects().size() - 2; i >= 0; i--) {
                                mutablePath.extend(fw2.objects().get(i), fw2.labels().get(i));
                            }
                            // Since the last path is directly attached to the ending node, we can just add it.
                            final Path p3 = fwInterface3.get(0);
                            for (int i = 1; i < p3.objects().size(); i++) {
                                mutablePath.extend(p3.objects().get(i), p3.labels().get(i));
                            }
                            intersectedPath.add(mutablePath);
                        }
                    }
                }

                System.out.println("Time taken: " + (Instant.now().toEpochMilli() - start.toEpochMilli()));
                System.out.println("fwInterface1: " + fwInterface1.size());
                System.out.println("fwInterface2: " + fwInterface2.size());
                System.out.println("intersectedPath: " + intersectedPath.size());
            } catch (Exception e) {
                e.printStackTrace();
            }
            executorService.shutdown();
        }
    }

    @Test
    public void fooV() {
        // App name search:
        //MATCH (srcVM:VM)-[srcHasInterface:HAS_INTERFACE]->(srcInterface:Interface) where srcVM.application = $src
        //WITH distinct srcInterface, srcVM
        //OPTIONAL MATCH (srcVM:VM)<-[srcHasVM:HAS_VM]-(srcHv:HyperVisor)
        //OPTIONAL MATCH (srcRack:Rack)-[srcHasHv:HAS_HYPERVISOR]->(srcHv:HyperVisor)
        //OPTIONAL MATCH (srcFloor:Floor)-[srcHasRack:HAS_RACK]-> (srcRack:Rack)
        //OPTIONAL MATCH (srcDC:Datacenter)-[srcHasFloor:HAS_FLOOR]->(srcFloor:Floor)
        //OPTIONAL MATCH (srcInterface:Interface)-[srcBelongsTo:BELONGS_TO_CIDR]->(srcCIDR:CIDR)
        //OPTIONAL MATCH (fwInterface:Interface)<-[sendsTraffic:SENDS_TRAFFIC ]-(srcCIDR)
        //WITH sendsTraffic, srcVM, srcDC, srcFloor, srcRack, srcHv, srcInterface
        //MATCH (fwInterface:Interface)-[forwardsTraffic:FORWARDS_TRAFFIC]->(destCIDR:CIDR)
        //WHERE sendsTraffic.ruleHash = forwardsTraffic.ruleHash
        //OPTIONAL MATCH (fwInterface)<-[:HAS_INTERFACE]-(firewall:Firewall)
        //WITH distinct destCIDR, sendsTraffic, srcVM, srcDC, srcFloor, srcRack, srcHv, srcInterface, firewall, fwInterface
        //MATCH (destVMInterface)-[destBelongsTo:BELONGS_TO_CIDR]->(destCIDR)
        //WITH distinct destVMInterface, destCIDR, sendsTraffic, srcVM, srcDC, srcFloor, srcRack, srcHv, srcInterface, firewall, fwInterface
        //MATCH (destVM:VM)-[destHasInterface:HAS_INTERFACE]->(destVMInterface) WHERE destVM.application = $dest
        //OPTIONAL MATCH (destDC:Datacenter)-[destHasFloor:HAS_FLOOR]->(destFloor:Floor)-[destHasRack:HAS_RACK]->(destRack:Rack)-[destHasHv:HAS_HYPERVISOR]->(destHv:HyperVisor)-[destHasVM:HAS_VM]->(destVM)

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal().with("evaluationTimeout", 24 * 60 * 60 * 1000);
            final List<Object> applicationOptimus = g.V().has("application", "Optimus").id().toList();

            //for (int i = 0; i < 5; i++) {
            //    Instant start2 = Instant.now();
            //    List<Object> ruleHashes = g.V(applicationOptimus).
            //            out("HAS_INTERFACE").dedup().
            //            out("BELONGS_TO_CIDR").dedup().
            //            outE("SENDS_TRAFFIC").values("ruleHash").dedup().toList();
            //    System.out.println("Time taken: " + (Instant.now().toEpochMilli() - start2.toEpochMilli()));
            //}

            for (int i = 0; i < 10; i++) {
                Instant start1 = Instant.now();

                final List<Path> test1 = g.V(applicationOptimus).as("srcVM").
                        out("HAS_INTERFACE").as("srcInterface").
                        out("BELONGS_TO_CIDR").as("srcCIDR").
                        outE("SENDS_TRAFFIC").as("sendsTraffic").
                        inV().hasLabel("Interface").as("fwInterface").path().by(T.id).by(T.id).by(T.id).by("ruleHash").by(T.id).toList();

                Set<Object> ruleHashes = test1.stream().map(p -> p.objects().get(p.size() - 2)).collect(Collectors.toSet());

                final List<Path> test2 = g.V().has("application", "DevOps").as("destVM").
                        out("HAS_INTERFACE").as("destInterface").
                        out("BELONGS_TO_CIDR").as("destCIDR").
                        inE("FORWARDS_TRAFFIC").as("forwardsTraffic").
                        outV().as("fwInterface").path().by(T.id).by(T.id).by(T.id).by("ruleHash").by(T.id).toList();
                List<Path> intersections = test2.stream().filter(p -> ruleHashes.contains(p.objects().get(p.size() - 2))).collect(Collectors.toList());

                System.out.println("Time taken: " + (Instant.now().toEpochMilli() - start1.toEpochMilli()));
            }
        }
    }

    @Test
    public void query4() {
        // App name search:
        //MATCH (srcVM:VM)-[srcHasInterface:HAS_INTERFACE]->(srcInterface:Interface) where srcVM.application = $src
        //WITH distinct srcInterface, srcVM
        //OPTIONAL MATCH (srcVM:VM)<-[srcHasVM:HAS_VM]-(srcHv:HyperVisor)
        //OPTIONAL MATCH (srcRack:Rack)-[srcHasHv:HAS_HYPERVISOR]->(srcHv:HyperVisor)
        //OPTIONAL MATCH (srcFloor:Floor)-[srcHasRack:HAS_RACK]-> (srcRack:Rack)
        //OPTIONAL MATCH (srcDC:Datacenter)-[srcHasFloor:HAS_FLOOR]->(srcFloor:Floor)
        //OPTIONAL MATCH (srcInterface:Interface)-[srcBelongsTo:BELONGS_TO_CIDR]->(srcCIDR:CIDR)
        //OPTIONAL MATCH (fwInterface:Interface)<-[sendsTraffic:SENDS_TRAFFIC ]-(srcCIDR)
        //WITH sendsTraffic, srcVM, srcDC, srcFloor, srcRack, srcHv, srcInterface
        //MATCH (fwInterface:Interface)-[forwardsTraffic:FORWARDS_TRAFFIC]->(destCIDR:CIDR)
        //WHERE sendsTraffic.ruleHash = forwardsTraffic.ruleHash
        //OPTIONAL MATCH (fwInterface)<-[:HAS_INTERFACE]-(firewall:Firewall)
        //WITH distinct destCIDR, sendsTraffic, srcVM, srcDC, srcFloor, srcRack, srcHv, srcInterface, firewall, fwInterface
        //MATCH (destVMInterface)-[destBelongsTo:BELONGS_TO_CIDR]->(destCIDR)
        //WITH distinct destVMInterface, destCIDR, sendsTraffic, srcVM, srcDC, srcFloor, srcRack, srcHv, srcInterface, firewall, fwInterface
        //MATCH (destVM:VM)-[destHasInterface:HAS_INTERFACE]->(destVMInterface) WHERE destVM.application = $dest
        //OPTIONAL MATCH (destDC:Datacenter)-[destHasFloor:HAS_FLOOR]->(destFloor:Floor)-[destHasRack:HAS_RACK]->(destRack:Rack)-[destHasHv:HAS_HYPERVISOR]->(destHv:HyperVisor)-[destHasVM:HAS_VM]->(destVM)
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal().with("evaluationTimeout", 24 * 60 * 60 * 1000);

            Instant start = Instant.now();

            //final Object application1Id = g.V().hasLabel("VM").has("application", "Electronic Toll Collection (ETOLL)").id().next();
            //final Object application2Id = g.V().hasLabel("VM").has("application", "<TODO app 2>").id().next();
            final List<Object> applicationOptimus = g.V().has("application", "Optimus").id().toList();

            final Object application1Id = 6143;
            final Object application2Id = 10060;


            Instant start1 = Instant.now();
            final List<Path> test1 = g.V(applicationOptimus).as("srcVM").
                    out("HAS_INTERFACE").as("srcInterface").
                    out("BELONGS_TO_CIDR").as("srcCIDR").
                    outE("SENDS_TRAFFIC").as("sendsTraffic").
                    inV().hasLabel("Interface").as("fwInterface").path().by(T.id).by(T.id).by(T.id).by("ruleHash").by(T.id).toList();
            System.out.println("Time taken: " + (Instant.now().toEpochMilli() - start1.toEpochMilli()));
            Set<Object> ruleHashes = test1.stream().map(p -> p.objects().get(p.size() - 2)).collect(Collectors.toSet());


            final List<Path> intersectedPaths = g.V().has("application", "DevOps").as("destVM").
                    out("HAS_INTERFACE").as("destInterface").
                    out("BELONGS_TO_CIDR").as("destCIDR").
                    inE("FORWARDS_TRAFFIC").as("forwardsTraffic").
                    where(__.values("ruleHash").is(P.within(ruleHashes))).
                    outV().as("fwInterface").path().by(T.id).by(T.id).by(T.id).by("ruleHash").by(T.id).toList();

            final List<Path> testIntersectedPaths = g.V().has("application", "DevOps").as("destVM").
                    out("HAS_INTERFACE").as("destInterface").
                    out("BELONGS_TO_CIDR").as("destCIDR").
                    inE("FORWARDS_TRAFFIC").as("forwardsTraffic").
                    where(__.values("ruleHash").is(P.within(Set.of("5935e9085033c3dc1d1e770a88ab08c2")))).
                    outV().as("fwInterface").path().by(T.id).by(T.id).by(T.id).by("ruleHash").by(T.id).toList();




            ExecutorService executorService = Executors.newFixedThreadPool(4);
            Future<List<Path>> output1 = executorService.submit(new RunQuery(g.V(application1Id).as("srcVM").
                    out("HAS_INTERFACE").as("srcInterface").
                    out("BELONGS_TO_CIDR").as("srcCIDR").
                    outE("SENDS_TRAFFIC").as("sendsTraffic").
                    inV().hasLabel("Interface").as("fwInterface").path()));

            Future<List<Path>> output2 = executorService.submit(new RunQuery(g.V(application2Id).as("destVM").
                    out("HAS_INTERFACE").as("destInterface").
                    out("BELONGS_TO_CIDR").as("destCIDR").
                    inE("FORWARDS_TRAFFIC").as("forwardsTraffic").
                    outV().as("fwInterface").path()));

            Future<List<Path>> output3 = executorService.submit(new RunQuery(
                    g.V(application1Id).as("vms").
                            in("HAS_VM").as("hvs").
                            in("HAS_HYPERVISOR").as("racks").
                            in("HAS_RACK").as("floors").
                            in("HAS_FLOOR").as("dcs").path()));

            Future<List<Path>> output4 = executorService.submit(new RunQuery(
                    g.V(application2Id).as("vms").
                            in("HAS_VM").as("hvs").
                            in("HAS_HYPERVISOR").as("racks").
                            in("HAS_RACK").as("floors").
                            in("HAS_FLOOR").as("dcs").path()));
            //OPTIONAL MATCH (destDC:Datacenter)-[:HAS_FLOOR]->(destFloor:Floor)-[:HAS_RACK]->(destRack:Rack)-[:HAS_HYPERVISOR]->(destHv:HyperVisor)-[:HAS_VM]->(destVM)

            try {
                List<Path> fwInterface1 = output1.get();
                List<Path> fwInterface2 = output2.get();
                List<Path> fwInterface3 = output3.get();
                List<Path> fwInterface4 = output4.get();
                assert fwInterface3.size() == 1;
                assert fwInterface4.size() == 1;
                List<Path> intersectedPath = new ArrayList<>();
                System.out.println("fwInterface1: " + fwInterface1.size());
                System.out.println("fwInterface2: " + fwInterface2.size());

                for (final Path fw1 : fwInterface1) {
                    final Object o1 = fw1.objects().get(fw1.objects().size() - 1);
                    for (final Path fw2 : fwInterface2) {
                        final Object o2 = fw2.objects().get(fw2.objects().size() - 1);
                        if (o1.equals(o2)) {
                            // Since the first path is directly attached to the starting node, we can just add it.
                            final Path mutablePath = MutablePath.make();
                            final Path p3 = fwInterface3.get(0);
                            for (int i = 1; i < p3.objects().size(); i++) {
                                mutablePath.extend(p3.objects().get(i), p3.labels().get(i));
                            }
                            final List<Object> objects = fw1.objects();
                            final List<Set<String>> labels = fw1.labels();
                            for (int i = 0; i < objects.size(); i++) {
                                mutablePath.extend(objects.get(i), labels.get(i));
                            }
                            for (int i = fw2.objects().size() - 2; i >= 0; i--) {
                                mutablePath.extend(fw2.objects().get(i), fw2.labels().get(i));
                            }
                            // Since the last path is directly attached to the ending node, we can just add it.
                            final Path p4 = fwInterface4.get(0);
                            for (int i = 1; i < p4.objects().size(); i++) {
                                mutablePath.extend(p4.objects().get(i), p4.labels().get(i));
                            }
                            intersectedPath.add(mutablePath);
                        }
                    }
                }

                System.out.println("Time taken: " + (Instant.now().toEpochMilli() - start.toEpochMilli()));
                System.out.println("fwInterface1: " + fwInterface1.size());
                System.out.println("fwInterface2: " + fwInterface2.size());

                // TODO: One of the paths is going to be backwards (either p3 or p4), most likely should just return values anyway.
                System.out.println("intersectedPath: " + intersectedPath.size());
            } catch (Exception e) {
                e.printStackTrace();
            }
            executorService.shutdown();
        }
    }

    @Test
    public void query5() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal().with("evaluationTimeout", 24 * 60 * 60 * 1000);

            Instant start = Instant.now();

            ExecutorService executorService = Executors.newFixedThreadPool(2);
            Future<List<Path>> output1 = executorService.submit(new RunQuery(g.V().hasLabel("Interface").has("ip", "10.194.18.160").as("srcInterface").
                    out("BELONGS_TO_CIDR").hasLabel("CIDR").as("srcCIDR")
                    .outE("SENDS_TRAFFIC").as("sendsTraffic")
                    .inV().hasLabel("Interface").as("fwInterface").path()));

            Future<List<Path>> output2 = executorService.submit(new RunQuery(g.V().hasLabel("Interface").has("ip", "10.1.20.208").as("destInterface").
                    out("BELONGS_TO_CIDR").as("destCIDR").
                    inE("FORWARDS_TRAFFIC").as("forwardsTraffic").
                    outV().as("fwInterface").path()));

            try {
                List<Path> fwInterface1 = output1.get();
                List<Path> fwInterface2 = output2.get();
                List<Path> intersectedPath = new ArrayList<>();
                System.out.println("fwInterface1: " + fwInterface1.size());
                System.out.println("fwInterface2: " + fwInterface2.size());

                for (final Path fw1 : fwInterface1) {
                    final Object o1 = fw1.objects().get(fw1.objects().size() - 1);
                    for (final Path fw2 : fwInterface2) {
                        final Object o2 = fw2.objects().get(fw2.objects().size() - 1);
                        if (o1.equals(o2)) {
                            final Path mutablePath = MutablePath.make();
                            final List<Object> objects = fw1.objects();
                            final List<Set<String>> labels = fw1.labels();
                            for (int i = 0; i < objects.size(); i++) {
                                mutablePath.extend(objects.get(i), labels.get(i));
                            }
                            for (int i = fw2.objects().size() - 2; i >= 0; i--) {
                                mutablePath.extend(fw2.objects().get(i), fw2.labels().get(i));
                            }
                            intersectedPath.add(mutablePath);
                        }
                    }
                }

                System.out.println("Time taken: " + (Instant.now().toEpochMilli() - start.toEpochMilli()));
                System.out.println("fwInterface1: " + fwInterface1.size());
                System.out.println("fwInterface2: " + fwInterface2.size());
                System.out.println("intersectedPath: " + intersectedPath.size());
            } catch (Exception e) {
                e.printStackTrace();
            }
            executorService.shutdown();
        }
    }


    @Test
    public void summary() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            final Map<Object, Object> summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
            System.out.println(summary);
        }
    }

    @Test
    public void q2() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

//            List vm = g.V(48780)
//                    .in()
//                    .in()
//                    .in() //.hasId(6143)
//                    //.outE()
////                    .values("ruleHash") //331514
//                    //.dedup()
//                    .limit(100)
//                    //.path()
//                    .toList();

            // 869 for 48780
            // 1227 for 25813
            List vm = g.V(6143)
                    .out().hasLabel("Interface") //6144
                    .out("BELONGS_TO_CIDR")
                    .out()//.hasId(25813)
                    .path()
                    .toList();
            System.out.println(vm.size());
        }
    }

    @Test
    public void query() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            List vm = g.V().has("environment", "DC-PROD")
                    .has("application18", "DevOps") //24
                    .out("HAS_INTERFACE") // interface 26
                    .out("BELONGS_TO_CIDR") // cidr 571
                    .dedup() //66
                    .as("srcCIDR")

                    .V().has("environment", "DC-PROD")
                    .has("application18", "DevOps")
                    .out("HAS_INTERFACE")
                    .out("BELONGS_TO_CIDR")
                    .dedup()
                    .as("destCIDR")
//                    .out("SENDS_TRAFFIC") // interface
//
//                    .limit(1)
//                    .out("FORWARDS_TRAFFIC") // cidr
//                    .in()
                    .project("FROM", "TO", "TRAFFIC")
                    .by(__.select("srcCIDR"))
                    .by(__.select("destCIDR"))
                    .by(__.select("srcCIDR")
                            .outE("SENDS_TRAFFIC").as("t")
                            .outV().hasLabel("Interface") //fwInterface
                            .out("FORWARDS_TRAFFIC")
                            .is(__.select("destCIDR"))
                            .select("t")
                    )
                    .toList();

            System.out.println(vm);

//            List result = g.V().hasLabel("Datacenter").as("srcDC")
//                    .out("HAS_FLOOR").hasLabel("Floor").as("srcFloor")
//                    .out("HAS_RACK").hasLabel("Rack").as("srcRack")
//                    .out("HAS_HYPERVISOR").hasLabel("HyperVisor").as("srcHv")
//                    .out("HAS_VM").hasLabel("VM").as("srcVM")
//                    .has("environment", "DC-PROD")
//                    .has("application", "App1")
//                    .out("HAS_INTERFACE").hasLabel("Interface").as("srcInterface").toList();
//            System.out.println(vm);
        }
    }

    @Test
    public void load() {
        //try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("172.17.0.1", 8182, "g"));
            g.V().drop().iterate();

            g.with("evaluationTimeout", 24 * 60 * 60 * 1000)
                    .call("aerospike.graphloader.admin.bulk-load.load")
                    .with("aerospike.graphloader.vertices", "/opt/aerospike-graph/etc/sampledata/vertices")
                    .with("aerospike.graphloader.edges", "/opt/aerospike-graph/etc/sampledata/edges").iterate();
        //}
    }

    @Test
    public void printErrors() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            // "duplicate-vertex-ids"
            // "bad-entries"
            // "bad-edges"
            final Iterator errors = (Iterator) g.call("aerospike.graphloader.admin.bulk-load.errors").with("type", "duplicate-vertex-ids").next();

            while (errors.hasNext()) {
                System.out.println(errors.next());
            }
        }
    }
}