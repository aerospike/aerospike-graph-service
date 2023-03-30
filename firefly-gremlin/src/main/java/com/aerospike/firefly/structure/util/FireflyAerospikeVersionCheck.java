package com.aerospike.firefly.structure.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.google.common.annotations.VisibleForTesting;

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

    FireflyAerospikeVersionCheck(final String version) {
        int extension1;
        if (version == null) {
            throw new IllegalArgumentException("Aerospike version cannot be null");
        }

        int begin = 0;
        int i = begin;
        int max = version.length();

        while (i < max) {
            if (! Character.isDigit(version.charAt(i))) {
                break;
            }
            i++;
        }

        major = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
        begin = ++i;

        while (i < max) {
            if (! Character.isDigit(version.charAt(i))) {
                break;
            }
            i++;
        }

        minor = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
        begin = ++i;

        while (i < max) {
            if (! Character.isDigit(version.charAt(i))) {
                break;
            }
            i++;
        }

        revision = (i > begin)? Integer.parseInt(version.substring(begin, i)) : 0;
        begin = i;
        final String extensionString = version.substring(begin + 1);
        if (extensionString.contains("-")) {
            extension1 = Integer.parseInt(extensionString.substring(0, extensionString.indexOf("-")));
        } else if (extensionString.contains("_")) {
            extension1 = Integer.parseInt(extensionString.substring(0, extensionString.indexOf("_")));
        } else {
            try {
                extension1 = Integer.parseInt(extensionString);
            } catch (final NumberFormatException e) {
                extension1 = 0;
            }
        }
        extension = extension1;
    }

    public static void validateVersion(final AerospikeClient client) {
        final Node node = client.getCluster().getRandomNode();
        final String response = Info.request(null, node, "build");
        final FireflyAerospikeVersionCheck version = new FireflyAerospikeVersionCheck(response);
        if (!validateVersion(version)) {
            throw new RuntimeException(String.format("Aerospike version %s is not supported. Minimum version is %s.%s.%s.%s",
                    version, MAJOR_MINIMUM, MINOR_MINIMUM, REVISION_MINIMUM, + EXTENSION_MINIMUM));
        }
    }

    static boolean validateVersion(final FireflyAerospikeVersionCheck version) {
        return version.major > MAJOR_MINIMUM ||
                (version.major == MAJOR_MINIMUM && (version.minor > MINOR_MINIMUM ||
                        (version.minor == MINOR_MINIMUM && (version.revision > REVISION_MINIMUM ||
                                (version.revision == REVISION_MINIMUM && version.extension >= EXTENSION_MINIMUM)))));
    }
}
