package com.aerospike.firefly.process;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyGraphProvider;
import com.aerospike.firefly.structure.FireflyGraphProviderUncached;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = FireflyGraphProviderUncached.class, graph = FireflyGraph.class)
public class FireflyGraphUncachedProcessStandardTest {
}
