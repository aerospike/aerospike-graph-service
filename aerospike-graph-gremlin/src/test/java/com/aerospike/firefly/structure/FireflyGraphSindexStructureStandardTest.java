package com.aerospike.firefly.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = FireflyGraphProviderSindex.class, graph = FireflyGraph.class)
public class FireflyGraphSindexStructureStandardTest {
}