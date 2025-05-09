package com.aerospike.firefly.io.aerospike.indexes;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.DataModelVersioning;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import com.aerospike.firefly.util.exceptions.ThreadLimitExceededException;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.lang.model.type.ExecutableType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestExceedThreadLimit extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private AerospikeConnection db;
    static AerospikeClient client;
    private FireflyGraph graph = null;

    @Before
    public void setup() {
        db = AerospikeConnection.connect(CONFIG);
        db.clearNamespace();
        ClientPolicy clientPolicy = new ClientPolicy();
        client = AerospikeConnection.setupDefaultClient(CONFIG, clientPolicy);
        graph = FireflyGraph.open(CONFIG);
        for(int i = 0;  i < 100000; i++){
            String label = "label" + i;
            graph.traversal().addV(label).next();
        }

        Node node   = client.getNodes()[0];
        InfoPolicy    infoPolicy = new InfoPolicy();

        String response = Info.request(
                infoPolicy,
                node,
                "set-config:context=service;query-threads-limit=1"
        );
        System.out.println("asinfo response: " + response);

        response = Info.request(
                infoPolicy,
                node,
                "get-config:context=service;query-threads-limit"
        );
        System.out.println("asinfo response: " + response);
    }

    @Test
    public void exceedThreadLimit() {
        int threads = 2;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(exec.submit(() -> {
                barrier.await();
                graph.traversal().V().toList();
                System.out.println("finished query");
                return null;
            }));
        }

        try{
            for (Future<Void> f : futures) {
                f.get();
            }
            Assert.fail("Should not have passed");
        }catch(InterruptedException e){
            throw new RuntimeException(e);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof ThreadLimitExceededException) {
                int code = ((ThreadLimitExceededException) cause).errorCode;
                int real = GraphError.THREAD_LIMIT_EXCEEDED.code;
                Assert.assertEquals(real, code);
            }
        }

        exec.shutdown();
        db.close();
    }
}
