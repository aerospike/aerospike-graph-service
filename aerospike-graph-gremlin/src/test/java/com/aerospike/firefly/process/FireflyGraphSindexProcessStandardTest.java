package com.aerospike.firefly.process;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyGraphProviderSindex;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = FireflyGraphProviderSindex.class, graph = FireflyGraph.class)
public class FireflyGraphSindexProcessStandardTest {
}
