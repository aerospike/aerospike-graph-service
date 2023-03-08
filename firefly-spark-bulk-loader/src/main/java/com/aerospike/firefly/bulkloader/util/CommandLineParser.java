package com.aerospike.firefly.bulkloader.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandLineParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineParser.class);
    static public CommandLine parseCmdArgs(final String[] args) {
        final Options options = new Options();
        final Option modeOption = new Option("m", "mode", true, "local when running in IDE, cluster when running spark-submit through CLI or in AWS");
        options.addOption(modeOption);

        final Option envOption = new Option("e", "env", true, " Optional argument. 'aws' when running job in cluster mode in AWS");
        options.addOption(envOption);

        final Option bucketOption = new Option("b", "bucket", true, "AWS S3 bucket name");
        options.addOption(bucketOption);

        final Option pathOption = new Option("c", "config", true, "config path [local/non-aws remote -> absolute/S3 -> full path to config.properties after bucket name]");
        options.addOption(pathOption);

        final org.apache.commons.cli.CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (final ParseException e) {
            LOGGER.error("Error parsing configuration arguments: ", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SparkBulkLoader", options);
            throw new IllegalArgumentException(e);
        }
    }
}
