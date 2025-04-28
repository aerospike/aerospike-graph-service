package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.runner.RunWith;

@RunWith(ProcessComputerSuite.class)
@GraphProviderClass(provider = DistributedGraphComputerProvider.class, graph = FireflyGraph.class)
public class DistributedGraphProcessComputerTest {
}

