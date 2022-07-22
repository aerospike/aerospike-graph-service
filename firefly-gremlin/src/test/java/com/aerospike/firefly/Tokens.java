package com.aerospike.firefly;

import java.nio.file.Path;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class Tokens {
    private Tokens() {
    }

    //Path is relative to module, ie ./firefly-gremlin/
    public static final Path INTEGRATION_TEST_PROPERTIES = Path.of("../conf/integration-test-settings.properties");
    public static final String AIR_ROUTES_50K_URL = "https://raw.githubusercontent.com/krlawrence/graph/master/sample-data/air-routes-latest.graphml";
    public static final String MOVIELENS_1M_URL = "http://files.phaseshift.studio/movielens-numericid.kryo";
}
