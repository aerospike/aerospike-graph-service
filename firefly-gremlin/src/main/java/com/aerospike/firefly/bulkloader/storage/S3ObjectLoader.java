package com.aerospike.firefly.bulkloader.storage;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class S3ObjectLoader implements ObjectLoader, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ObjectLoader.class);
    private static S3ObjectLoader s3ObjectLoader;
    private final AmazonS3 s3Client;
    private String bucketName;

    private S3ObjectLoader() {
        s3Client = AmazonS3ClientBuilder.standard().build();
    }

    /**
     * Create a singleton instance of S3ObjectLoader to be used across all the distributed compute Spark map transformations
     *
     * @return S3ObjectLoader
     */
    public static synchronized S3ObjectLoader getInstance() {
        if (s3ObjectLoader == null) {
            s3ObjectLoader = new S3ObjectLoader();
        }
        return s3ObjectLoader;
    }

    public void setBucketName(String bucketName) {
        s3ObjectLoader.bucketName = bucketName;
    }

    /**
     * Function to load config file from S3.
     *
     * @param configPath Path to config in bucket.
     * @return Configuration object built from config file.
     */
    @Override
    public Map<String, Object> loadConfiguration(final String configPath) {
        try (final S3Object s3Object = this.s3Client.getObject(bucketName, configPath);
             final InputStream inputStream = s3Object.getObjectContent()) {
            final Properties props = new Properties();
            props.load(inputStream);
            final HashMap<String, Object> configData = new HashMap<>();
            props.keySet().forEach(it -> {
                final String key = it.toString().toLowerCase();
                final Object value = props.get(it.toString());
                LOGGER.debug("config[{}:{}]", key, value);
                configData.put(key, value);
            });
            return configData;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Function to load input files from S3.
     * This function returns all the paths of the csv files.
     *
     * @param directory Folder key string specifying the name of the master directory of vertices or edges.
     * @return List of key string of the csv files in S3.
     */
    @Override
    public List<String> getCsvPaths(final String directory) throws IOException {
        // TODO GRAPH-491: Test this in an AWS environment to ensure nested csv files are properly accounted for.
        try {
            final List<String> keys = new ArrayList<>();
            ObjectListing response = this.s3Client.listObjects(bucketName, directory);
            List<S3ObjectSummary> objects = response.getObjectSummaries();
            for (final S3ObjectSummary object : objects) {
                keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
            }
            // listObjects loads 1000 object keys in one call.
            // If there are multiple directories with more than 1000 files, then need to consume any remaining objects.
            while (response.isTruncated()) {
                response = this.s3Client.listNextBatchOfObjects(response);
                objects = response.getObjectSummaries();
                for (S3ObjectSummary object : objects) {
                    keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
                }
            }
            return keys;
        } catch (final SdkClientException e) {
            throw new IOException(e);
        }
    }
}
