package com.aerospike.firefly.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = FireflyGraphProviderUncached.class, graph = FireflyGraph.class)
public class FireflyGraphUncachedStructureStandardTest {
}