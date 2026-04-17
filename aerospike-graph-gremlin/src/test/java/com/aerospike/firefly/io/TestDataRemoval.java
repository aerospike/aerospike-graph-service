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

package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.IOUtil;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.util.Util.verifyClean;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

public class TestDataRemoval extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    private static final File tempFile;
    private static final URL airRoutesUrl;

    static {
        try {
            airRoutesUrl = new URL(AIR_ROUTES_50K_URL);
            tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "air-routes50k.graphml");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void smallDataRemoval(){
        FireflyGraph graph = FireflyGraph.open(config);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
        graph.traversal().V().drop().iterate();
        verifyClean(graph);
    }

    @Test
    public void largerDataRemoval() throws IOException {
        FireflyGraph graph = FireflyGraph.open(config);
        if (!tempFile.exists()) IOUtil.downloadFileFromURL(airRoutesUrl, tempFile);
        graph.io(graphml()).readGraph(tempFile.getAbsolutePath());
        graph.traversal().V().drop().iterate();
        verifyClean(graph);
    }

}
