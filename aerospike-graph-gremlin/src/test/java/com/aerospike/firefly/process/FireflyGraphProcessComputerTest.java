package com.aerospike.firefly.process;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

@RunWith(ProcessComputerSuite.class)
@GraphProviderClass(provider = FireflyGraphComputerProvider.class, graph = FireflyGraph.class)
public class FireflyGraphProcessComputerTest {
}

