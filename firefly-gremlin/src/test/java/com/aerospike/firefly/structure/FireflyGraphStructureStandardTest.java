package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = FireflyGraphProvider.class, graph = FireflyGraph.class)
public class FireflyGraphStructureStandardTest {
    private Configuration conf;
    private AerospikeConnection db;




    @AfterEach
    void closeGraphClearData() throws Exception {
        this.conf = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
        this.db = AerospikeConnection.connect(ConfigurationHelper.aerospikeHost(conf),ConfigurationHelper.aerospikePort(conf),ConfigurationHelper.aerospikeNamespace(conf));
        db.dropDatabase();
    }
}