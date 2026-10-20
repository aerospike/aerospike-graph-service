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

package com.aerospike.firefly.bulkloader.integration.util;

import org.apache.commons.io.FileUtils;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Deletes files written to disk by bulk loader tests, both before and after the guarded scope so that output left
 * behind by an interrupted run is removed as well.
 * <p>
 * Every test class that runs the bulk loader should declare {@link #tempDirectory()} as a {@code @Rule}. Test classes
 * that generate input data in {@code @BeforeClass} should also declare the matching generated data cleanup as a
 * {@code @ClassRule}.
 */
public class BulkLoaderTestCleanup extends ExternalResource {
    /**
     * Root of every {@code aerospike.graphloader.temp-directory} used by test configs. Test configs must point their
     * temp directory below this root, otherwise the output is not cleaned up.
     */
    public static final String TEMP_DIRECTORY_ROOT = "src/test/resources/conf/packed/temp";

    private final List<File> paths;

    private BulkLoaderTestCleanup(final String... paths) {
        this.paths = Arrays.stream(paths).map(File::new).collect(Collectors.toList());
    }

    public static BulkLoaderTestCleanup tempDirectory() {
        return new BulkLoaderTestCleanup(TEMP_DIRECTORY_ROOT);
    }

    /** Output of {@code src/test/resources/csv-generate.py}. */
    public static BulkLoaderTestCleanup recoveryCsvData() {
        return new BulkLoaderTestCleanup(
                "src/test/resources/recoverydata/vertices",
                "src/test/resources/recoverydata/edges");
    }

    /** Output of {@code src/test/resources/csv-generate-summary-count.py}. */
    public static BulkLoaderTestCleanup summaryCountCsvData() {
        return new BulkLoaderTestCleanup("src/test/resources/sampledata-incremental-supernode-count");
    }

    @Override
    protected void before() {
        delete();
    }

    @Override
    protected void after() {
        delete();
    }

    private void delete() {
        for (final File path : paths) {
            try {
                if (path.exists()) {
                    FileUtils.forceDelete(path);
                }
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to delete bulk loader test output " + path, e);
            }
        }
    }
}
