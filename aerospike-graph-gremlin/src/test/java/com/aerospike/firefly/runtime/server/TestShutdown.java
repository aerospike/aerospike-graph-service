/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
