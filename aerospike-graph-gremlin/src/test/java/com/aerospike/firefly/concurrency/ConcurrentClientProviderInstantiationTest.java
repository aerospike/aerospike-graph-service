package com.aerospike.firefly.concurrency;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class ConcurrentClientProviderInstantiationTest {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

    @Test
    public void testConcurrentInstantiation() throws InterruptedException {
        Thread t1 = new Thread(new InstantiateFirefly());
        Thread t2 = new Thread(new InstantiateFirefly());
        Thread t3 = new Thread(new InstantiateFirefly());
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
    }

    private static class InstantiateFirefly implements Runnable {

        @Override
        public void run() {
            final FireflyGraph graph = FireflyGraph.open(CONFIG);
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            graph.close();
        }
    }
}
