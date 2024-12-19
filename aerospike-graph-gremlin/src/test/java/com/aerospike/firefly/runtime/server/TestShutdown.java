package com.aerospike.firefly.runtime.server;

import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.util.OutputCapturer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestShutdown {

    @Test
    public void testGracefulShutdown() throws IOException, InterruptedException {
        FireflyServer server = null;

        try (final OutputCapturer outputCapturer = new OutputCapturer()) {

            server = FireflyServer.start(new String[]{"../conf/firefly-gremlin-server-multi-tenant.yaml"});
            server.stop().join();
            Thread.sleep(10);
            server = null;

            final String[] logList = outputCapturer.getLines();

            boolean graph0Closed = false;
            boolean graphModernClosed = false;
            boolean metricsClosed = false;

            for (final String line : logList) {
                if (line.contains("Closing FireflyGraph 0.")) {
                    graph0Closed = true;
                } else if (line.contains("Closing FireflyGraph modern.")) {
                    graphModernClosed = true;
                } else if (line.contains("Shutting down server metrics.")) {
                    metricsClosed = true;
                }
            }
            assertTrue(graph0Closed);
            assertTrue(graphModernClosed);
            assertTrue(metricsClosed);
        } finally {
            if (server != null) {
                server.stop().join();
            }
        }
    }
}
