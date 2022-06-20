package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.After;
import org.junit.runner.RunWith;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = FireflyGraphProvider.class, graph = FireflyGraph.class)
public class TestFireflyGraphStructureStandard {
    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
    }


    @After
    public void closeGraphClearData() {
        AerospikeConnection db = AerospikeConnection.connect(config);
        db.dropDatabase();
        db.close();
    }
}