package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

// @RunWith(ProcessComputerSuite.class)
@GraphProviderClass(provider = LocalGraphComputerProvider.class, graph = FireflyGraph.class)
public class LocalGraphProcessComputerTest {
}

