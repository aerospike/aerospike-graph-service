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
import org.junit.After;
import org.junit.Test;



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
 * @author Connor Hengstler
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
    private int originalThreadLimit;

    @Before
    public void setup() {
        db = AerospikeConnection.connect(CONFIG);
        db.clearNamespace();
        ClientPolicy clientPolicy = new ClientPolicy();
        client = AerospikeConnection.setupDefaultClient(CONFIG, clientPolicy);
        graph = FireflyGraph.open(CONFIG);
        for(int i = 0;  i < 10000; i++){
            String label = "label" + i;
            graph.traversal().addV(label).next();
        }

        Node node   = client.getNodes()[0];
        InfoPolicy    infoPolicy = new InfoPolicy();

        String getResponse = Info.request(
                infoPolicy,
                node,
                "get-config:context=service;query-threads-limit"
        );
        int parsedLimit = -1;
        for (String opt : getResponse.split(";")) {
            if (opt.startsWith("query-threads-limit=")) {
                String[] parts = opt.split("=", 2);
                parsedLimit = Integer.parseInt(parts[1].trim());
                originalThreadLimit = parsedLimit;
                break;
            }
        }

        if (parsedLimit < 0) {
            throw new IllegalStateException("Could not find query-threads-limit in response: " + getResponse);
        }

        String response = Info.request(
                infoPolicy,
                node,
                "set-config:context=service;query-threads-limit=1"
        );
        response = Info.request(
                infoPolicy,
                node,
                "get-config:context=service;query-threads-limit"
        );
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
            }else{
                Assert.fail(cause.getMessage());
            }
        }

        exec.shutdown();
        db.close();
    }

    @After
    public void teardown() {
        Node node = client.getNodes()[0];
        InfoPolicy infoPolicy = new InfoPolicy();

        String restoreCmd =
                "set-config:context=service;query-threads-limit=" + originalThreadLimit;
        String restoreResponse = Info.request(infoPolicy, node, restoreCmd);
        String verify = Info.request(
                infoPolicy,
                node,
                "get-config:context=service;query-threads-limit"
        );
        graph.close();
        db.close();
    }
}
