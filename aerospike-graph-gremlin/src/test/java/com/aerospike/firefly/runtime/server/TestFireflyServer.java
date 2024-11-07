package com.aerospike.firefly.runtime.server;

import com.aerospike.firefly.runtime.FireflyServer;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestFireflyServer {

    @Test
    public void testServer() throws Exception {
        FireflyServer server = FireflyServer.start(new String[] {"../conf/firefly-gremlin-server-local.yaml"});
        Thread.sleep(60 * 1000 * 1000);
    }
}
