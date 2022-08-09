package com.aerospike.firefly.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
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

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestMovielensLoader extends AbstractFireflySuite {
    private static final String MOVIELENS_TMP = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "movielens" + System.getProperty("file.separator");
    private static final String MOVIELENS_BASEPATH = MOVIELENS_TMP + System.getProperty("file.separator") + "ml-1m";

    //@BeforeClass
    //public static void openGraphFetchData() {
    //    try {
    //        URL movieLensUrl = new URL(MOVIELENS_URL);
    //        File tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "ml-1m.zip");
    //        if (!tempFile.exists()) {
    //            IOUtil.downloadFileFromURL(movieLensUrl, tempFile);
    //            Unzip.unzip(tempFile.getAbsolutePath(), MOVIELENS_TMP);
    //        }
    //    } catch (IOException e) {
    //        throw new RuntimeException(e);
    //    }
    //}
    //
    //@Test
    //public void testLoadMovieLens() {
    //    Movielens.parse(Path.of(MOVIELENS_BASEPATH), graph);
    //    final Long verticesLoaded = graph.traversal().V().count().next();
    //    final Long edgesLoaded = graph.traversal().E().count().next();
    //    System.out.println(String.format("movielens vertex count [%d] edge count [%d]", verticesLoaded, edgesLoaded));
    //    graph.traversal().V().drop().iterate();
    //    verifyClean(graph);
    //}

}
