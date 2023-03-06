package com.aerospike.firefly.bulkloader.config;

import org.apache.commons.configuration2.Configuration;

public class S3ObjectLoader implements ConfigLoader {
    private final String bucketName;

    public S3ObjectLoader(String bucketName) {
        this.bucketName = bucketName;
    }
    @Override
    public Configuration loadConfigFile(String directoryPath) {
        return null;
    }
}
