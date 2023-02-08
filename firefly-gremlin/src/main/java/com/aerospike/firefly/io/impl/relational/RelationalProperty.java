package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyProperty;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class RelationalProperty<V> extends FireflyProperty<V> {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalProperty.class);
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
            graph.removeProperty(fireflyElement, key());

            // Need to make sure cached properties are removed from Edge.
            if (fireflyElement instanceof FireflyEdge) {
                ((FireflyEdge) fireflyElement).removeCachedProperty(key());
            }
        } catch (final AerospikeException ae) {
            // Removing a property that is already removed SHOULD NOT yield an error.
            if (ae.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                LOG.debug("Ignored exception removing an already-removed property {}.", this, ae);
            } else {
                throw ae;
            }
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
                db.setFromElementType(element.getClass()),
                element.id,
                db.PROPERTIES,
                key,
                value,
                db.TYPE_HINTS);
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
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.setFromElementType(element.getClass()), element.id);
        if (fireflyRecord == null)
            return new TreeMap<>();

        final Map<String, Property<V>> result = new TreeMap<>();
        final Map<String, Object> properties = (Map<String, Object>) fireflyRecord.record.getMap(db.PROPERTIES);
        final Map<String, Long> typeHint = (Map<String, Long>) fireflyRecord.record.getMap(db.TYPE_HINTS);
        if (properties == null)
            return result;
        properties.forEach((key, value) -> {
            Property<V> property = new RelationalProperty<>(
                    graph,
                    element,
                    key,
                    (V) db.convertValuetoTypeUsingHint(value, typeHint.get(key)));
            result.put(key, property);
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
                        db.setFromElementType(element.getClass()),
                        element.id,
                        db.PROPERTIES,
                        key,
                        db.TYPE_HINTS));
    }
}
