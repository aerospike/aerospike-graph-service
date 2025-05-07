package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyAerospikeVersionCheck {
    // Note. The AerospikeClient has a Version checker, but it does not check the extension version, and
    // we need to error if the extension version is not high enough so it does not work for us.
    private static final int MAJOR_MINIMUM = 6;
    private static final int MINOR_MINIMUM = 2;
    private static final int REVISION_MINIMUM = 0;
    private static final int EXTENSION_MINIMUM = 7;

    private final int major;
    private final int minor;
    private final int revision;
    private final int extension;
    private static boolean versionLogged = false;

    private static final Logger LOG = LoggerFactory.getLogger(FireflyAerospikeVersionCheck.class);

    public FireflyAerospikeVersionCheck(final String version){
        if (version == null) {
            throw new IllegalArgumentException("Aerospike version cannot be null");
        }

        if (!versionLogged) {
            LOG.info("Aerospike version: {}.", version);
            versionLogged = true;
        }

        String[] parts = version.split("\\.");
        major = Integer.parseInt(parts[0]);
        if(major >= 10){ // Epoch Semantic Version, nothing matters but the major
            minor = 0;
            revision = 0;
            extension = 0;
            return;
        }
        minor = Integer.parseInt(parts[1]);
        revision = Integer.parseInt(parts[2]);
        extension = Integer.parseInt(parts[3].substring(0, 1));
    }

    public static void validateVersion(final IAerospikeClient client, final boolean requireMRTSupport) {
        for (final Node node : client.getNodes()) {
            LOG.debug("Info.request: build");
            final String response = Info.request(null, node, "build");
            final FireflyAerospikeVersionCheck version = new FireflyAerospikeVersionCheck(response);
            if (!validateVersion(version)) {
                throw new RuntimeException(String.format("Aerospike version %d.%d.%d.%d is not supported. Minimum version is %s.%s.%s.%s." +
                                " Please verify that all nodes in the cluster are running a compatible version of Aerospike.",
                        version.major, version.minor, version.revision, version.extension,
                        MAJOR_MINIMUM, MINOR_MINIMUM, REVISION_MINIMUM, EXTENSION_MINIMUM));
            }

            if (requireMRTSupport && version.major < 8) {
                throw new RuntimeException(String.format("Aerospike version %d.%d.%d.%d is not supported. Minimum version 8 required for MRT." +
                                " Please verify that all nodes in the cluster are running a compatible version of Aerospike.",
                        version.major, version.minor, version.revision, version.extension));
            }
        }
    }

    public static boolean validateVersion(final FireflyAerospikeVersionCheck version) {
        return version.major > MAJOR_MINIMUM ||
                (version.major == MAJOR_MINIMUM && (version.minor > MINOR_MINIMUM ||
                        (version.minor == MINOR_MINIMUM && (version.revision > REVISION_MINIMUM ||
                                (version.revision == REVISION_MINIMUM && version.extension >= EXTENSION_MINIMUM)))));
    }

}
