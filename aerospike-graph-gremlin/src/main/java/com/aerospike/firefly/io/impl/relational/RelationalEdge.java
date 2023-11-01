package com.aerospike.firefly.io.impl.relational;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class RelationalEdge extends FireflyEdge {

    /**
     * Constructor for RelationalEdge.
     *
     * @param fid       FireflyId to use.
     * @param label     Edge label.
     * @param graph     FireflyGraph to use.
     * @param outVertex Edge out vertex.
     * @param inVertex  Edge in vertex.
     */
    public RelationalEdge(final FireflyId fid,
                          final String label,
                          final FireflyGraph graph,
                          final FireflyId outVertex,
                          final FireflyId inVertex,
                          final Map<String, Object> properties,
                          final Map<String, Object> typeHints) {
        super(fid, label, graph, inVertex, outVertex, properties, typeHints);
    }

}
