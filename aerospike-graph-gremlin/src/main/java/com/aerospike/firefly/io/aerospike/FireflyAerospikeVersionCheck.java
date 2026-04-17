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

package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.firefly.util.exceptions.AerospikeCompoundIndexNotSupportedException;
import com.aerospike.firefly.util.exceptions.AerospikeMrtNotSupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FireflyAerospikeVersionCheck {
    // Note. The AerospikeClient has a Version checker, but it does not check the extension version, and
    // we need to error if the extension version is not high enough so it does not work for us.
    private static final int MAJOR_MINIMUM = 6;
    private static final int MINOR_MINIMUM = 2;
    private static final int REVISION_MINIMUM = 0;
    private static final int EXTENSION_MINIMUM = 7;
    private static final Logger LOG = LoggerFactory.getLogger(FireflyAerospikeVersionCheck.class);
    private static boolean versionLogged = false;
    private final int major;
    private final int minor;
    private final int revision;
    private final int extension;

    public FireflyAerospikeVersionCheck(final String version) {
        if (version == null) {
            throw new IllegalArgumentException("Aerospike version cannot be null");
        }

        if (!versionLogged) {
            LOG.info("Aerospike version: {}.", version);
            versionLogged = true;
        }

        final String[] parts = version.split("\\.");

        this.major = Integer.parseInt(parts[0]);
        if (this.major >= 10) { // Epoch Semantic Version, extension omitted in this version
            this.minor = Integer.parseInt(parts[1]);
            this.revision = parseNumberFromString(parts[2]);
            this.extension = 0;
        } else {
            this.minor = Integer.parseInt(parts[1]);
            this.revision = Integer.parseInt(parts[2]);
            this.extension = parseNumberFromString(parts[3]);
        }
    }

    private static int parseNumberFromString(final String parse) {
        final StringBuilder builder = new StringBuilder();
        for (final char c : parse.toCharArray()) {
            if (Character.isDigit(c)) {
                builder.append(c);
            } else {
                break;
            }
        }

        return Integer.parseInt(builder.toString());
    }

    /**
     * Validates the Aerospike cluster version and returns the minimum version found across all nodes.
     * This version object can be stored and used later for feature support checks.
     *
     * @param client The Aerospike client
     * @return The minimum version found across all nodes in the cluster
     */
    public static FireflyAerospikeVersionCheck validateAndGetClusterVersion(final IAerospikeClient client) {
        FireflyAerospikeVersionCheck minVersion = null;
        for (final Node node : client.getNodes()) {
            LOG.debug("Info.request: build");
            // Cannot set up InfoPolicy here because AerospikeConnection does not exist yet, so use default.
            final String response = Info.request(null, node, "build");
            final FireflyAerospikeVersionCheck version = new FireflyAerospikeVersionCheck(response);
            if (!validateVersion(version)) {
                throw new RuntimeException(String.format("Aerospike version %d.%d.%d.%d is not supported. Minimum version is %s.%s.%s.%s." +
                                " Please verify that all nodes in the cluster are running a compatible version of Aerospike.",
                        version.major, version.minor, version.revision, version.extension,
                        MAJOR_MINIMUM, MINOR_MINIMUM, REVISION_MINIMUM, EXTENSION_MINIMUM));
            }
            if (minVersion == null || version.compareTo(minVersion) < 0) {
                minVersion = version;
            }
        }
        return minVersion;
    }

    public boolean supportsMRT() {
        return this.major >= 8;
    }

    public boolean supportsExpressionIndexes() {
        return this.major > 8 || (this.major == 8 && this.minor >= 1);
    }

    public void requireMRTSupport() {
        if (!supportsMRT()) {
            throw new AerospikeMrtNotSupportedException();
        }
    }

    public void requireExpressionIndexSupport() {
        if (!supportsExpressionIndexes()) {
            throw new AerospikeCompoundIndexNotSupportedException();
        }
    }

    /**
     * Compare versions. Returns negative if this version is less than other,
     * positive if greater, 0 if equal.
     */
    public int compareTo(final FireflyAerospikeVersionCheck other) {
        if (this.major != other.major) return Integer.compare(this.major, other.major);
        if (this.minor != other.minor) return Integer.compare(this.minor, other.minor);
        if (this.revision != other.revision) return Integer.compare(this.revision, other.revision);
        return Integer.compare(this.extension, other.extension);
    }

    public static boolean validateVersion(final FireflyAerospikeVersionCheck version) {
        return version.major > MAJOR_MINIMUM ||
                (version.major == MAJOR_MINIMUM && (version.minor > MINOR_MINIMUM ||
                        (version.minor == MINOR_MINIMUM && (version.revision > REVISION_MINIMUM ||
                                (version.revision == REVISION_MINIMUM && version.extension >= EXTENSION_MINIMUM)))));
    }

}
