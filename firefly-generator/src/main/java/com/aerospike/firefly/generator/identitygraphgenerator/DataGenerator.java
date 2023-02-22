package com.aerospike.firefly.generator.identitygraphgenerator;

import com.aerospike.firefly.generator.identitygraphgenerator.IdentityGenerator.Builder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DataGenerator {
    private final static Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    public static void main(String[] args) {
        LOG.info("Main thread is - " + Thread.currentThread().getName());
        try {
            final CommandLine cmd = parseCmdArgs(args);
            final String numOfHouseholds = cmd.hasOption("h") ? cmd.getOptionValue("h") : "2";
            Builder builder = Builder.create();
            builder = builder.opsPerTransaction(50000)
                    .cmdLineArgs(cmd)
                    .households(Integer.parseInt(numOfHouseholds))
                    .accountsPerHousehold(20)
                    .peoplePerHousehold(10)
                    .devicesPerPerson(3);
            IdentityGenerator identityGenerator = builder.generate();
            identityGenerator.run();
            LOG.info("Data generator finished. Exiting.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static public CommandLine parseCmdArgs(final String[] args) {
        final Options options = new Options();
        final Option envOption = new Option("e", "env", true, "local/remote or aws");
        options.addOption(envOption);

        final Option pathOption = new Option("d", "directory", true, "Optional param. Absolute path to directory to write the datagenerator output in local." +
                " If AWS bucket, please provide the sub directory name within the bucket");
        checkAndSetRequired(pathOption, args);
        options.addOption(pathOption);

        final Option bucketOption = new Option("b", "bucket", true, "AWS S3 bucket name only if running in AWS");
        checkAndSetRequired(bucketOption, args);
        options.addOption(bucketOption);

        final Option accesskeyOption = new Option("a", "awsAccessKey", true, "AWS AccessKey option when running the jar in AWS to write to S3");
        checkAndSetRequired(accesskeyOption, args);
        options.addOption(accesskeyOption);

        final Option secretKeyOption = new Option("s", "awsSecretKey", true, "AWS SecretKey option when running the jar in AWS to write to S3");
        checkAndSetRequired(secretKeyOption, args);
        options.addOption(secretKeyOption);

        final Option numOfHouseholdsOption = new Option("h", "numOfHouseholds", true, "Number of house holds to generate data for. default 500 for local data genration");
        numOfHouseholdsOption.setRequired(false);
        options.addOption(numOfHouseholdsOption);

        final Option recordsPerFileOption = new Option("r", "recordsPerFile", true, "Number of records to store per file. Default is 100 for local");
        recordsPerFileOption.setRequired(false);
        options.addOption(recordsPerFileOption);

        final Option varianceOption = new Option("v", "variance", true, "'variance' for Gaussian distribution");
        varianceOption.setRequired(false);
        options.addOption(varianceOption);

        final CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (final ParseException e) {
            LOG.error("Error parsing configuration arguments: ", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("DataGenerator", options);
            throw new IllegalArgumentException(e);
        }
    }

    public static void checkAndSetRequired(Option option, String[] args) {
        if(!ArrayUtils.contains(args, "local"))
            option.setRequired(true);
    }
}
