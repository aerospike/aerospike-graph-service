package com.aerospike.firefly.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(CustomStructureStandardSuite.class)
@GraphProviderClass(provider = FireflyGraphProvider.class, graph = FireflyGraph.class)
public class FireflyCustomGraphStructureStandardTest {
}
