package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.structure.SparkFireflyVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.*;

public class BulkLoader {
    private static final Logger logger = LoggerFactory.getLogger(BulkLoader.class);
    private static final String[] REQUIRED_VERTEX_HEADERS = new String[]{ID_HEADER};
    private static final String[] REQUIRED_EDGE_HEADERS = new String[]{ID_HEADER, "~from", "~to"};
//    private static final String DEFAULT_CONFIG_PATH = "/Users/mbelsare/Documents/code/aerospike/firefly/conf/spark-bulk-loader-conf/config.properties";
//    private static final String DEFAULT_CONFIG_PATH = "/Users/mbelsare/Documents/code/aerospike/firefly/conf/spark-bulk-loader-conf/config-new.properties";
    private static final String DEFAULT_CONFIG_PATH = "conf/config-new.properties";
    private static Configuration config;

    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();


    public static void main(final String[] args) {
        // TODO Spark: What is the right way of configuring a Spark Application? If a config.properties is not it, how
        //             should it be done?
        final Path path;
        System.out.println("Inside Main of Bulkloader");
        if (args.length < 1) {
            //throw new IllegalArgumentException("A configuration file was not provided");
            // TODO: Remove this in production as a default is not valid
            path = Path.of(DEFAULT_CONFIG_PATH);
        } else {
            path = Path.of(args[0]);
        }
//        config = getConfig(path);
        config = getConfigFromS3(DEFAULT_CONFIG_PATH);

//        final List<String> vertexFiles = validateAndGetFiles(getOrDefault(VERTEX_DIRECTORY_KEY, config));
        final Set<String> vertexFiles = getObjectslistFromFolder("bulk-loader-spark", "vertexes");
//        final List<String> edgeFiles = validateAndGetFiles(getOrDefault(EDGE_DIRECTORY_KEY, config));
        final Set<String> edgeFiles = getObjectslistFromFolder("bulk-loader-spark", "edges");

        // Initialize Spark
        final SparkSession spark = SparkSession.builder().appName("firefly-bulk-loader").getOrCreate();
        final List<Dataset<Row>> vertexDatasets = new ArrayList<>();
        final List<Dataset<Row>> edgeDatasets = new ArrayList<>();


        // TODO Spark: Is this a good way to verify required headers exist in all files beforehand?
        for (String vertexFile : vertexFiles) {
            final Dataset<Row> vertexData = spark.read().format("csv").option("header", true).load(vertexFile);
            final Set<String> headers = new HashSet<>();
            for (final String header : vertexData.columns())
                headers.add(header.toLowerCase());
            for (final String requiredHeader : REQUIRED_VERTEX_HEADERS) {
                if (!headers.contains(requiredHeader))
                    throw new IllegalArgumentException("Unable to find all required column header values in source: " +
                            vertexFile);
            }
            vertexDatasets.add(vertexData);
        }

        for (String edgeFile : edgeFiles) {
            final Dataset<Row> edgeData = spark.read().format("csv").option("header", true).load(edgeFile);
            final Set<String> headers = new HashSet<>();
            for (final String header : edgeData.columns())
                headers.add(header.toLowerCase());
            for (final String requiredHeader : REQUIRED_EDGE_HEADERS) {
                if (!headers.contains(requiredHeader))
                    throw new IllegalArgumentException("Unable to find all required column header values in source: " +
                            edgeFile);
            }
            edgeDatasets.add(edgeData);
        }

        // Vertexes
        for (final Dataset<Row> vertexData : vertexDatasets) {
            JavaRDD<Row> rdd = vertexData.toJavaRDD();
            List<Integer> collectRDD = rdd.mapPartitions((FlatMapFunction<Iterator<Row>, Integer>) rowIterator -> {
                var new_config = getConfigFromS3(DEFAULT_CONFIG_PATH);
                try (final FireflyGraph graph = FireflyGraph.open(new_config)) {
                    System.out.println("Running insert for each vertex datasets");
                    while (rowIterator.hasNext()) {
                        final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                        try {
                            final SparkFireflyVertex sparkVertex = SparkFireflyVertex.createVertex(row,
                                    Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, config)));
                            graph.writeVertex(sparkVertex.getId(), sparkVertex.getLabel(), sparkVertex.getProperties());
                            graph.close();
                        } catch (final RuntimeException e) {
                            final boolean ignoreElementCreationFailed =
                                    Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, config));
                            if (!ignoreElementCreationFailed) {
                                throw e;
                            }
                        }
                    }
                }
                return Collections.singletonList(1).iterator();
            }).collect();
//            vertexData.foreachPartition((Iterator<Row> csvIterator) -> {
//                var new_config = getConfigFromS3(DEFAULT_CONFIG_PATH);
//                try (final FireflyGraph graph = FireflyGraph.open(new_config)) {
//                    System.out.println("Running insert for each vertex datasets");
//                    while (csvIterator.hasNext()) {
//                        final GenericRowWithSchema row = (GenericRowWithSchema) csvIterator.next();
//                        try {
//                            final SparkFireflyVertex sparkVertex = SparkFireflyVertex.createVertex(row,
//                                    Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, config)));
//                            graph.writeVertex(sparkVertex.getId(), sparkVertex.getLabel(), sparkVertex.getProperties());
//                            graph.close();
//                        } catch (final RuntimeException e) {
//                            final boolean ignoreElementCreationFailed =
//                                    Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, config));
//                            if (!ignoreElementCreationFailed) {
//                                throw e;
//                            }
//                        }
//                    }
//                }
//            });
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

        if (directories.length != 0)
            return Arrays.asList(directories).stream().map(x-> x.getAbsolutePath()).collect(Collectors.toList());
        return Collections.singletonList(directory);
    }

    public static Set<String> getObjectslistFromFolder(String bucketName, String folderKey) {
        Set<String> keys = new HashSet<>();
        ListObjectsV2Result response = s3Client.listObjectsV2(bucketName, folderKey);
        List<S3ObjectSummary> objects = response.getObjectSummaries();

        for (S3ObjectSummary object : objects)
            keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
        return keys;
    }

    static public Configuration getConfigFromS3(String path) {
        return loadFromS3(path);
    }

    public static Configuration loadFromS3(final String path) {
        return loadFileFromS3(path);
    }

    public static Configuration loadFileFromS3(final String path) {
        try {
            Properties props = new Properties();
            S3Object s3Object = s3Client.getObject("bulk-loader-spark", path);
            InputStream is = s3Object.getObjectContent();
            props.load(is);
            HashMap<String, Object> configData = new HashMap<>();
            props.keySet().forEach(it -> {
                final String key = it.toString().toLowerCase();
                final Object value = props.get(it.toString());
                logger.debug("config[{}:{}]", key, value);
                configData.put(key, value);
            });
            return new MapConfiguration(configData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

