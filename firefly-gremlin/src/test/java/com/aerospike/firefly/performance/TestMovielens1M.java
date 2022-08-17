package com.aerospike.firefly.performance;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import com.aerospike.firefly.util.Movielens;
import com.aerospike.firefly.util.Unzip;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.io.Util.verifyClean;
import static com.aerospike.firefly.util.Movielens.MOVIELENS_URL;
import static com.aerospike.firefly.util.Movielens.YEAR;
import static org.junit.Assert.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestMovielens1M extends AbstractFireflySuite {

    private static final String MOVIELENS_TMP = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "movielens" + System.getProperty("file.separator");
    private static final String MOVIELENS_BASEPATH = MOVIELENS_TMP + System.getProperty("file.separator") + "ml-1m";
    private GraphTraversalSource g;

    @BeforeClass
    public static void fetchData() {
        try {
            URL movieLensUrl = new URL(MOVIELENS_URL);
            File tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "ml-1m.zip");
            if (!tempFile.exists()) {
                IOUtil.downloadFileFromURL(movieLensUrl, tempFile);
                Unzip.unzip(tempFile.getAbsolutePath(), MOVIELENS_TMP);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Movielens.parse(Path.of(MOVIELENS_BASEPATH), FireflyGraph.open(config));
    }

    @AfterClass
    public static void clearDataAfterTest() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            graph.getBaseGraph().dropDatabase();
        }
    }

    @Before
    public void createTraversal() {
        g = graph.traversal();
    }

    @Test
    public void testYearExtraction() {
        String title = "Gilda (1946)";
        String[] tokens = title.split("[()]");
        assertEquals(tokens.length, 2);
        assertEquals(tokens[tokens.length - 1], "1946");
    }

    @Test
    public void testQueryMovieLens1M() {
        long vCountStart = System.currentTimeMillis();
        long vCount = graph.traversal().V().count().next();
        long vCountEnd = System.currentTimeMillis();
        long searchPeopleRatedGilda = g.V().hasLabel("person").out().has("name", "Gilda (1946)").count().next();
        long sprgEnd = System.currentTimeMillis();
        long searchGildaRatedByPeople = g.V().has("name", "Gilda (1946)").inE().outV().count().next();
        long sgrbpEnd = System.currentTimeMillis();
        assertEquals(searchPeopleRatedGilda, searchGildaRatedByPeople);
        long oneMovie = g.V().has(YEAR, 1946).has("name", "Gilda (1946)").count().next();
        assertEquals(1L, oneMovie);
        LOG.info("Vertex count {} time {} seconds", vCount, (vCountEnd - vCountStart) / 1000);
        LOG.info("searchPeopleRatedGilda time {} seconds", (sprgEnd - vCountEnd) / 1000);
        LOG.info("searchGildaRatedByPeople time {} seconds", (sgrbpEnd - sprgEnd) / 1000);
    }

    @Test
    public void testLoadMovieLens() {
        final Long verticesLoaded = graph.traversal().V().count().next();
        final Long edgesLoaded = graph.traversal().E().count().next();
        System.out.println(String.format("movielens vertex count [%d] edge count [%d]", verticesLoaded, edgesLoaded));
        graph.traversal().V().drop().iterate();
        verifyClean(graph);
    }
}
