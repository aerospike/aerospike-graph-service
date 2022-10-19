package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.structure.SparkFireflyVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.util.*;

import static com.aerospike.firefly.bulkloader.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.*;

public class BulkLoader {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoader.class);
    private static final String[] REQUIRED_VERTEX_HEADERS = new String[]{ID_HEADER};
    private static final String[] REQUIRED_EDGE_HEADERS = new String[]{ID_HEADER, "~from", "~to"};
    private static final String DEFAULT_CONFIG_PATH = "conf/spark-bulk-loader-conf/config.properties";
//    private static final String DEFAULT_CONFIG_PATH = "/Users/mbelsare/Documents/code/aerospike/firefly/conf/spark-bulk-loader-conf/config-new.properties";
    private static Configuration config;

    public static void main(final String[] args) {
        // TODO Spark: What is the right way of configuring a Spark Application? If a config.properties is not it, how
        //             should it be done?
        final Path path;
        if (args.length < 1) {
            //throw new IllegalArgumentException("A configuration file was not provided");
            // TODO: Remove this in production as a default is not valid
            path = Path.of(DEFAULT_CONFIG_PATH);
        } else {
            path = Path.of(args[0]);
        }
        config = getConfig(path);
        // TODO Spark: Support csv data sources that Spark can support such as S3
        final List<String> vertexFiles = validateAndGetFiles(getOrDefault(VERTEX_DIRECTORY_KEY, config));
        final List<String> edgeFiles = validateAndGetFiles(getOrDefault(EDGE_DIRECTORY_KEY, config));

        // Initialize Spark
        final SparkSession spark = SparkSession.builder().master("local").appName("firefly-bulk-loader").getOrCreate();
        final List<Dataset<Row>> vertexDatasets = new ArrayList<>();
        final List<Dataset<Row>> edgeDatasets = new ArrayList<>();

        // TODO Spark: Is this a good way to verify required headers exist in all files beforehand?
        for (final String vertexFile : vertexFiles) {
            final Dataset<Row> vertexData = spark.read().format("csv").option("header", true).load(vertexFile);
            final Set<String> headers = new HashSet<>();
            for (final String header : vertexData.columns()) {
                headers.add(header.toLowerCase());
            }
            for (final String requiredHeader : REQUIRED_VERTEX_HEADERS) {
                if (!headers.contains(requiredHeader)) {
                    throw new IllegalArgumentException("Unable to find all required column header values in source: " +
                            vertexFile);
                }
            }
            vertexDatasets.add(vertexData);
        }
        for (final String edgeFile : edgeFiles) {
            final Dataset<Row> edgeData = spark.read().format("csv").option("header", true).load(edgeFile);
            final Set<String> headers = new HashSet<>();
            for (final String header : edgeData.columns()) {
                headers.add(header.toLowerCase());
            }
            for (final String requiredHeader : REQUIRED_EDGE_HEADERS) {
                if (!headers.contains(requiredHeader)) {
                    throw new IllegalArgumentException("Unable to find all required column header values in source: " +
                            edgeFile);
                }
            }
            edgeDatasets.add(edgeData);
        }

        // Vertexes
        for (final Dataset<Row> vertexData : vertexDatasets) {
            vertexData.foreachPartition((Iterator<Row> csvIterator) -> {
                try (final FireflyGraph graph = FireflyGraph.open(config)) {
                    while (csvIterator.hasNext()) {
                        final GenericRowWithSchema row = (GenericRowWithSchema) csvIterator.next();
                        try {
                            final SparkFireflyVertex sparkVertex = SparkFireflyVertex.createVertex(row,
                                    Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, config)));
                            graph.writeVertex(sparkVertex.getId(), sparkVertex.getLabel(), sparkVertex.getProperties());
                        } catch (final RuntimeException e) {
                            final boolean ignoreElementCreationFailed =
                                    Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, config));
                            if (!ignoreElementCreationFailed) {
                                throw e;
                            }
                        }
                    }
                }
            });
        }

        // TODO: Edges
        spark.stop();
    }

    static private List<String> validateAndGetFiles(final String directory) {
        File file = new File(directory);
        File[] directories = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        });

        assert directories != null;
        if (directories.length != 0)
            return List.of(Arrays.toString(directories));
        return Collections.singletonList(directory);
    }
}

