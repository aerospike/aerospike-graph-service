package com.aerospike.firefly.blogs;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

public class BidirectionalSearchBlog {

    @Test
    public void loadFlights() throws MalformedURLException {
        // Download flights.
        final File tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "air-routes50k.graphml");
        final URL airRoutesUrl = new URL(AIR_ROUTES_50K_URL);
        if (!tempFile.exists()) IOUtil.downloadFileFromURL(airRoutesUrl, tempFile);

        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            // Drop existing database.
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();

            // Load flights.
            System.out.println("Loading Air Routes 50k");
            graph.io(graphml()).readGraph(tempFile.getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void bidirectionalSearchPart1() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            final GraphTraversalSource g = graph.traversal();

            final List<Map<String, Object>> intersection = g.
                    V().has("code", "YQQ").as("A").
                    V().has("code", "NAP").as("B").
                    repeat(
                            __.choose(__.loops().math("_ % 2").is(P.eq(0)),
                                    __.select("A").out("route").dedup().as("A"),
                                    __.select("B").in("route").dedup().as("B"))
                    ).
                    until(
                                __.select("A").id().as("a-code").
                                        select("B").id().as("b-code").
                                        select("a-code", "b-code").
                                        where("a-code", P.eq("b-code")).count().is(P.gt(0))).
                    project("path", "intersection-code").
                    by(
                            __.path().by("code")).
                    by(
                            __.select("A").values("code")).
                    toList();

            for (final Map<String, Object> map : intersection) {
                final Path path = (Path) map.get("path");
                final String start = path.get(0);
                final String end = path.get(1);
                final List<String> middleLeft = new ArrayList<>();
                final List<String> middleRight = new ArrayList<>();

                // Due to the way the traversal is written, the path goes A0, B0, A0, A1, B0, B1, A1, A2, ...
                for (int i = 3; i < path.size(); i += 4) {
                    middleLeft.add(path.get(i));
                }

                // Due to the way the traversal is written, the path goes A0,  B0,   A0,  A1,  B0,  B1,  A1,  A2,  B1, B2
                //                                                        YQQ, NAP, YQQ, YVR, NAP, EWR, YVR, AUS, EWR, AUS
                for (int i = 5; i < path.size(); i += 4) {
                    middleRight.add(path.get(i));
                }

                final List<String> completePath = new ArrayList<>();
                assert !middleRight.isEmpty();
                assert !middleLeft.isEmpty();
                Collections.reverse(middleRight);

                if (middleLeft.get(middleLeft.size() - 1).equals(middleRight.get(0))) {
                    System.out.println("Intersection - " + middleLeft.get(middleLeft.size() - 1));
                    // Don't want to duplicate the intersection
                    middleRight.remove(0);
                }

                // Middle right is in reverse order
                completePath.add(start);
                completePath.addAll(middleLeft);
                completePath.addAll(middleRight);
                completePath.add(end);
                System.out.println(completePath);
            }
        }
    }
}
