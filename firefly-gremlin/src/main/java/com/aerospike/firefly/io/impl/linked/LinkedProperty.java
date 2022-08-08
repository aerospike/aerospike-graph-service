package com.aerospike.firefly.io.impl.linked;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class LinkedProperty<V> extends FireflyProperty<V> {
    private final FireflyGraph graph;
    private final FireflyElement fireflyElement;

    public LinkedProperty(final FireflyGraph graph, final FireflyElement element, final String key, final V value) {
        super(element, key, value);
        this.graph = graph;
        this.fireflyElement = element;
    }

    @Override
    public void remove() {
        final AerospikeConnection db = graph.getBaseGraph();
        db.removeTypeHintedValueFromMap(db.getElementPropertySet(fireflyElement.getClass()),
                FireflyId.fromElement(fireflyElement), db.getElementPropertySet(fireflyElement.getClass()), key());
        graph.removeProperty(fireflyElement, key());
    }

    public static <V> Property<V> writeProperty(final FireflyGraph graph, final FireflyElement element, final String key, final V value) {
        final AerospikeConnection db = graph.getBaseGraph();
        FireflyHelper.validatePropertyValue(value);
        db.writeTypeHintedValueToMap(
                db.getElementPropertySet(element.getClass()),
                element.id,
                db.getElementPropertySet(element.getClass()),
                key,
                value);
        return new LinkedProperty<V>(graph, element, key, value);
    }

    public static <V> Map<String, Property<V>> readProperties(final FireflyGraph graph, final FireflyElement element) {
        final AerospikeConnection db = graph.getBaseGraph();
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.getElementPropertySet(element.getClass()), element.id.toNumericId());
        if (fireflyRecord == null)
            return new HashMap<>();

        final Map<String, Property<V>> result = new HashMap<>();
        final Map<String, Object> data = (Map<String, Object>) fireflyRecord.record.getMap(db.getElementPropertySet(element.getClass()));
        if (data == null)
            return result;
        data.forEach((key1, value) -> {
            Property<V> prop = readProperty(graph, element, key1);
            result.put(key1, prop);
        });
        return result;
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * construct and return a Property from the value associated with k in the ELEMENT_PROPERTIES map
     *
     * @param element Element to read property from
     * @param key     property key
     * @param <V>     type
     * @return Property
     */
    public static <V> Property<V> readProperty(final FireflyGraph graph, final FireflyElement element, final String key) {
        final AerospikeConnection db = graph.getBaseGraph();
        return new LinkedProperty<>(graph, element, key,
                db.readTypeHintedValueFromMap(
                        db.getElementPropertySet(element.getClass()),
                        FireflyId.fromElement(element).toNumericId(),
                        db.getElementPropertySet(element.getClass()),
                        key));
    }
}
