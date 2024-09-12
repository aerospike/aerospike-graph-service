package com.aerospike.firefly.process;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyGraphProviderAdjacentUserId;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Simon Zhao (<a href="https://github.com/DKZed">https://github.com/DKZed</a>)
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = FireflyGraphProviderAdjacentUserId.class, graph = FireflyGraph.class)
public class FireflyGraphAdjacentUserIdProcessStandardTest {
}
