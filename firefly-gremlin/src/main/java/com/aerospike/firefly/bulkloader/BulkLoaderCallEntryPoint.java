package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.process.call.FireflyBulkLoaderServiceFactory;

import java.util.ArrayList;
import java.util.List;


public class BulkLoaderCallEntryPoint implements FireflyBulkLoaderServiceFactory.BulkLoad {
    @Override
    public void perform(final String configPath, final boolean aws, final boolean vertices, final boolean edges) {
        if (configPath == null) {
            throw new IllegalArgumentException("Config file path cannot be null.");
        }

        final List<String> args = new ArrayList<>();

        // This will be local as far as spark is concerned.
        args.add("-m");
        args.add("local");

        // Add config path.
        args.add("-c");
        args.add(configPath);

        if (aws) {
            // If aws set environment ('-e') to 'aws'.
            args.add("-e");
            args.add("aws");
        }

        // Always dry run and detect supernodes.
        args.add("-dryrun");
        args.add("-supernode");

        if (vertices) {
            // If we are loading vertices, add write/verify step.
            args.add("-writevertex");
            args.add("-verifyvertex");
        }

        if (edges) {
            // If we are loading edges, add write/verify steps.
            args.add("-writeedge");
            args.add("-verifyedge");
        }

        SparkBulkLoaderMain.main(args.toArray(new String[0]));
    }
}
