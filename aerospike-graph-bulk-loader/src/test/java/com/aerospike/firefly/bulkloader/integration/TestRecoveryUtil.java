package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TestRecoveryUtil {

    private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config.properties";

    @Test
    public void testInfo() {
        try (FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(DEFAULT_CONFIG))) {
            RecoveryUtil.truncate(graph);

            graph.traversal().V().drop().iterate();

            for (int i = 0; i < 50_000; i++) {
                RecoveryUtil.writeVertexPartitionComplete(graph.getBaseGraph(), i);
                RecoveryUtil.writeEdgePartitionComplete(graph.getBaseGraph(), i);
            }
            Set<Object> supernodes = new HashSet<>();
            int stringIdx = 0;
            int intIdx = 0;
            for (int i = 0; i < 50_000; i++) {
                Random random = new Random();
                if (random.nextBoolean()) {
                    intIdx++;
                    supernodes.add(i);
                } else {
                    stringIdx++;
                    supernodes.add(String.valueOf(i));
                }
            }
            RecoveryUtil.writeSupernodeList(graph.getBaseGraph(), supernodes);
            RecoveryUtil.RecoveryInfo recoveryInfo = RecoveryUtil.recover(graph.getBaseGraph());
            Long i = 0L;
            for (i = 0L; i < 50_000L; i++) {
                if (!recoveryInfo.getVertexPartitions().contains(i)) {
                    System.out.println("Vertex partition " + i + " not found in recovery info");
                }
                if (!recoveryInfo.getEdgePartitions().contains(i)) {
                    System.out.println("Edge partition " + i + " not found in recovery info");
                }
            }
            Assert.assertEquals(50_000, recoveryInfo.getVertexPartitions().size());
            Assert.assertEquals(50_000, recoveryInfo.getEdgePartitions().size());
            Assert.assertEquals(50_000, recoveryInfo.getSupernodes().size());
            for (Object d : recoveryInfo.getSupernodes()) {
                if (d instanceof String) {
                    stringIdx--;
                } else {
                    intIdx--;
                }
            }
            Assert.assertEquals(0, stringIdx);
            Assert.assertEquals(0, intIdx);
        }
    }

    @Test
    public void testState() {
        try (FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(DEFAULT_CONFIG))) {
            RecoveryUtil.truncate(graph);

            RecoveryUtil.updateState(graph.getBaseGraph(), RecoveryUtil.RecoveryState.DETECT_SUPERNODES);
            RecoveryUtil.RecoveryInfo recoveryInfo = RecoveryUtil.recover(graph.getBaseGraph());
            Assert.assertEquals(RecoveryUtil.RecoveryState.DETECT_SUPERNODES.name(), recoveryInfo.getState());

            RecoveryUtil.updateState(graph.getBaseGraph(), RecoveryUtil.RecoveryState.VERTEX_WRITE);
            recoveryInfo = RecoveryUtil.recover(graph.getBaseGraph());
            Assert.assertEquals(RecoveryUtil.RecoveryState.VERTEX_WRITE.name(), recoveryInfo.getState());

            RecoveryUtil.updateState(graph.getBaseGraph(), RecoveryUtil.RecoveryState.VERTEX_VERIFY);
            recoveryInfo = RecoveryUtil.recover(graph.getBaseGraph());
            Assert.assertEquals(RecoveryUtil.RecoveryState.VERTEX_VERIFY.name(), recoveryInfo.getState());

            RecoveryUtil.updateState(graph.getBaseGraph(), RecoveryUtil.RecoveryState.EDGE_WRITE);
            recoveryInfo = RecoveryUtil.recover(graph.getBaseGraph());
            Assert.assertEquals(RecoveryUtil.RecoveryState.EDGE_WRITE.name(), recoveryInfo.getState());

            RecoveryUtil.updateState(graph.getBaseGraph(), RecoveryUtil.RecoveryState.EDGE_VERIFY);
            recoveryInfo = RecoveryUtil.recover(graph.getBaseGraph());
            Assert.assertEquals(RecoveryUtil.RecoveryState.EDGE_VERIFY.name(), recoveryInfo.getState());

            RecoveryUtil.truncate(graph);
            recoveryInfo = RecoveryUtil.recover(graph.getBaseGraph());
            Assert.assertNull(recoveryInfo.getState());
        }
    }
}
