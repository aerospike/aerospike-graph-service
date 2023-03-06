package com.aerospike.firefly.bulkloader.storage;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class S3ObjectLoader implements ObjectLoader {
    private final Logger LOGGER = LoggerFactory.getLogger(S3ObjectLoader.class);
    private final String bucketName;
    private final AmazonS3 S3_CLIENT;

    public S3ObjectLoader(String bucketName, AmazonS3 S3_CLIENT) {
        this.bucketName = bucketName;
        this.S3_CLIENT = S3_CLIENT;
    }

    /**
     * Function to load config file from S3.
     *
     * @param configPath Path to config in bucket.
     * @return Configuration object built from config file.
     */
    @Override
    public Configuration loadConfigFile(final String configPath) {
        try (final S3Object s3Object = this.S3_CLIENT.getObject(bucketName, configPath);
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
            return new MapConfiguration(configData);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Function to get a set of valid sub-directory strings of vertices or edges from the bucket.
     *
     * @param directory directory path after bucket name.
     * @return The set of valid S3 paths for sub-directories within edges/vertices.
     */
    @Override
    public Set<String> getObjectList(final String directory) {
        try {
            final Set<String> keys = new HashSet<>();
            ObjectListing response = S3_CLIENT.listObjects(bucketName, directory);
            List<S3ObjectSummary> objects = response.getObjectSummaries();
            for (final S3ObjectSummary object : objects) {
                keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
            }
            // listObjects loads 1000 object keys in one call.
            // If there are multiple directories with more than 1000 files, then need to consume any remaining objects.
            while (response.isTruncated()) {
                response = S3_CLIENT.listNextBatchOfObjects(response);
                objects = response.getObjectSummaries();
                for (S3ObjectSummary object : objects) {
                    keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
                }
            }
            return keys;
        } catch (final SdkClientException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Function to load input files from S3.
     * This function returns all the directory paths leading upto the csv files. Does not return the csv's.
     *
     * @param bucketName Name of the S3 bucket.
     * @param folderKey  Folder key string specifying the name of the master directory of verticies or edges.
     * @return Set of directory path strings containing the csv files in S3.
     */
    public Set<String> getObjectsListFromS3(final String bucketName, final String folderKey) {
        final Set<String> keys = new HashSet<>();
        ObjectListing response = this.S3_CLIENT.listObjects(bucketName, folderKey);
        List<S3ObjectSummary> objects = response.getObjectSummaries();
        for (final S3ObjectSummary object : objects) {
            keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
        }
        // listObjects loads 1000 object keys in one call.
        // If there are multiple directories with more than 1000 files, then need to consume any remaining objects.
        while (response.isTruncated()) {
            response = S3_CLIENT.listNextBatchOfObjects(response);
            objects = response.getObjectSummaries();
            for (S3ObjectSummary object : objects) {
                keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
            }
        }
        return keys;
    }
}
