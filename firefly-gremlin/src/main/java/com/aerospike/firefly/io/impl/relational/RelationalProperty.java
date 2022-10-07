package com.aerospike.firefly.io.impl.relational;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.utils.GenerationCheck;
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
public class RelationalProperty<V> extends FireflyProperty<V> {
    private final FireflyGraph graph;
    private final FireflyElement fireflyElement;

    /**
     * Constructor for RelationalProperty.
     *
     * @param graph   Graph that property exists in.
     * @param element Element that property exists on.
     * @param key     Key of property.
     * @param value   Value of property.
     */
    public RelationalProperty(final FireflyGraph graph, final FireflyElement element, final String key, final V value) {
        super(element, key, value);
        this.graph = graph;
        this.fireflyElement = element;
    }

    /**
     * Remove this property from the graph.
     */
    @Override
    public void remove() {
        try {
            final AerospikeConnection db = graph.getBaseGraph();
            GenerationCheck.writeGenerationCheck(() -> db.removeTypeHintedValueFromMap(
                                                                 db.getElementPropertySet(fireflyElement.getClass()),
                                                                 FireflyId.fromElement(fireflyElement),
                                                                 db.getElementPropertySet(fireflyElement.getClass()), key()));
            graph.removeProperty(fireflyElement, key());
        } catch (Exception ignored) {
            // Removing a property that is already removed SHOULD NOT yield an error.
        }
    }

    /**
     * Write a single property to the element.
     *
     * @param graph   Graph to write to.
     * @param element Element to add property to.
     * @param key     Key of property.
     * @param value   Value of property.
     * @param <V>     Type of property.
     * @return Property that was written.
     */
    public static <V> Property<V> writeProperty(final FireflyGraph graph, final FireflyElement element, final String key, final V value) {
        // RelationalProperty property writes are atomic since they only hit 1 record. Generation check within
        // writeTypeHintedValueToMap ensures correctness.
        final AerospikeConnection db = graph.getBaseGraph();
        FireflyHelper.validatePropertyValue(value);
        db.writeTypeHintedValueToMap(
                db.getElementPropertySet(element.getClass()),
                element.id,
                db.getElementPropertySet(element.getClass()),
                key,
                value);
        return new RelationalProperty<>(graph, element, key, value);
    }

    /**
     * Read properties from element.
     *
     * @param graph   Graph to read from.
     * @param element Element to read properties of.
     * @param <V>     Type of property value.
     * @return Map of label to properties.
     */
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
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET.
     * construct and return a Property from the value associated with k in the ELEMENT_PROPERTIES map.
     *
     * @param element Element to read property from.
     * @param key     property key.
     * @param <V>     type.
     * @return Property.
     */
    public static <V> Property<V> readProperty(final FireflyGraph graph, final FireflyElement element, final String key) {
        final AerospikeConnection db = graph.getBaseGraph();
        return new RelationalProperty<>(graph, element, key,
                db.readTypeHintedValueFromMap(
                        db.getElementPropertySet(element.getClass()),
                        FireflyId.fromElement(element).toNumericId(),
                        db.getElementPropertySet(element.getClass()),
                        key));
    }
}
