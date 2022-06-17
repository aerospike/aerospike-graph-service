package com.aerospike.firefly.util;

import com.aerospike.firefly.io.AerospikeConnection;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class PreflightCheckHelper {
    private PreflightCheckHelper() {}
    private static final Integer JAVA_ELEVEN = 11;
    private static final List<Integer> supportedJVMVersions = new ArrayList<>() {{
        add(JAVA_ELEVEN);
    }};

    public static void checkSupportedJVM() {
        String version = System.getProperty("java.version");
        if (version.startsWith("1.")) {
            version = version.substring(2, 3);
        } else {
            int dot = version.indexOf(".");
            if (dot != -1) {
                version = version.substring(0, dot);
            }
        }
        int versionNumber = Integer.parseInt(version);
        if (!supportedJVMVersions.contains(versionNumber))
            throw new RuntimeException(String.format("Firefly does not support java version %d", versionNumber));
    }

    public static void checkAerospikeEnterprise(AerospikeConnection ac) {
        if (!ac.aerospikeEnterprise())
            throw new RuntimeException("Firefly requires aerospike Enterprise Edition");
    }
}
