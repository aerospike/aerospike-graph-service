package com.aerospike.firefly.io.impl.standard;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.Backend;
import com.aerospike.firefly.io.AbstractBackend;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ElementBackend extends AbstractBackend implements Backend.Element {

    public ElementBackend(AerospikeConnection db) {
        super(db);
    }

    /**
     * write a new property into the property Record for element
     * 1 property record per element, a Map bin of name to value
     *
     * @param id    Element id to write to
     * @param clazz Element type
     * @param key   property key
     * @param value property value to write
     * @param <V>   type
     */
    @Override
    public <V> void writeProperty(final FireflyId id, final Class<? extends FireflyElement> clazz, final String key, final V value) {
        FireflyHelper.validatePropertyValue(value);
        db.writeTypeHintedValueToMap(db.getElementPropertySet(clazz), id, db.getElementPropertySet(clazz), key, value);
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * remove k from the ELEMENT_PROPERTIES map
     *
     * @param element Element to remove property from
     * @param key     property key to remove
     * @param <V>     type
     */
    @Override
    public <V> void removeProperty(final FireflyElement element, final String key) {
        db.removeTypeHintedValueFromMap(db.getElementPropertySet(element.getClass()), FireflyId.fromElement(element), db.getElementPropertySet(element.getClass()), key);
    }

    /**
     * get a list of currently valid ids
     *
     * @param type type of Element
     * @return Iterator of raw Ids
     */
    @Override
    public Iterator<?> readElementIds(final Class<? extends FireflyElement> type) {
        final AerospikeConnection.IdConfig cfg = new AerospikeConnection.IdConfig(db, type);
        return db.scanAllIdsInSet(cfg.getAeroSet());
    }

    /**
     * Read the set of properties for an associated element
     * A record with the id of its element is read from the Aerospike set PROPERTY_AERO_SET
     *
     * @param element element to read properties from
     * @param <V>     type
     * @return Map of Label to Property
     */

    @Override
    public <V> Map<String, Property> readProperties(final FireflyElement element) {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.getElementPropertySet(element.getClass()), FireflyId.fromElement(element).toNumericId());
        if (fireflyRecord == null)
            return new HashMap<>();

        final Map<String, Property> result = new HashMap<>();
        final Map<String, V> data = (Map<String, V>) fireflyRecord.record.getMap(db.getElementPropertySet(element.getClass()));
        if (data == null)
            return result;
        data.forEach((key1, value) -> {
            Property<V> prop = readProperty(element, key1);
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
    @Override
    public <V> Property readProperty(final FireflyElement element, final String key) {
        return new FireflyProperty(element, key, db.readTypeHintedValueFromMap(db.getElementPropertySet(element.getClass()), FireflyId.fromElement(element).toNumericId(), db.getElementPropertySet(element.getClass()), key));
    }

}
