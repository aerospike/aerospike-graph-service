package com.aerospike.firefly.process;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyGraphProvider;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.runner.RunWith;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = FireflyGraphProvider.class, graph = FireflyGraph.class)
public class FireflyGraphProcessStandardTest {
    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
    }

    @AfterEach
    void closeGraphClearData() {
        AerospikeConnection db = AerospikeConnection.connect(
                ConfigurationHelper.aerospikeHost(config),
                ConfigurationHelper.aerospikePort(config),
                ConfigurationHelper.aerospikeNamespace(config));
        db.dropDatabase();
        db.close();
    }
}
