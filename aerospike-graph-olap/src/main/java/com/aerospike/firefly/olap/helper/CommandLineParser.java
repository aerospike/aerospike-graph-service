/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.olap.helper;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.LOCAL_MODE;

public class CommandLineParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineParser.class);

    public static final String CONFIG_DIRECTORY_KEY = "aerospike.graph-olap.config";

    public static final Map<String, String> KEY_TO_CMD = Map.ofEntries(
            Map.entry(CONFIG_DIRECTORY_KEY, "c")
    );

    static public CommandLine parseCmdArgs(final String[] args) {
        final Options options = new Options();
        final Option modeOption = new Option(LOCAL_MODE, "Flag to indicate job is running from IDE/JVM");
        options.addOption(modeOption);
        final Option config = new Option(KEY_TO_CMD.get(CONFIG_DIRECTORY_KEY), CONFIG_DIRECTORY_KEY, true, "Path to config. Local: Absolute path. AWS S3: Full path after bucket name.");
        config.setRequired(true);
        options.addOption(config);
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
