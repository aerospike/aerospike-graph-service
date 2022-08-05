package com.aerospike.firefly.io.impl.linked;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyProperty;
import com.aerospike.firefly.structure.id.FireflyId;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class LinkedProperty<V> extends FireflyProperty<V> {
    private final FireflyGraph graph;
    private final FireflyElement fireflyElement;

    public LinkedProperty(FireflyGraph graph, FireflyElement element, String key, V value) {
        super(element, key, value);
        this.graph = graph;
        this.fireflyElement = element;
    }

    @Override
    public void remove() {
        final AerospikeConnection db = graph.getBaseGraph();
        db.removeTypeHintedValueFromMap(db.getElementPropertySet(fireflyElement.getClass()), FireflyId.fromElement(fireflyElement), db.getElementPropertySet(fireflyElement.getClass()), key());
        graph.removeProperty(fireflyElement, key());
    }
}
