package com.aerospike.firefly.bulkloader.structure;

import com.aerospike.firefly.structure.id.FireflyId;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class SparkFireflyElement implements Serializable {
    public static final String ID_HEADER = "~id";
    protected static final String LABEL_HEADER = "~label";

    protected final long id;
    protected final String label;
    protected final List<Map.Entry<String, Object>> properties;

    protected SparkFireflyElement(final long id, final String label,
                                final List<Map.Entry<String, Object>> properties) {
        this.id = id;
        this.label = label;
        this.properties = properties;
    }

    public abstract FireflyId getId();

    public String getLabel() {
        return this.label;
    }

    public List<Map.Entry<String, Object>> getProperties() {
        return this.properties;
    }

    protected static Map.Entry<String, Object> generateProperty(final String header, final String value) {
        // TODO: Cardinality support?
        final int typeSpecifierIndex = header.lastIndexOf(":");
        if (typeSpecifierIndex == -1) {
            return new AbstractMap.SimpleEntry<>(header, value);
        } else {
            String propertyName = header.substring(0, typeSpecifierIndex);
            String type = header.substring(typeSpecifierIndex + 1);
            boolean isList = false;
            if (type.endsWith("[]")) {
                isList = true;
                type = type.substring(0, type.length() - 2);
            }
            final Object propertyValue;
            switch (type.toLowerCase()) {
                case "long":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(Long::parseLong).collect(Collectors.toList());
                    } else {
                        propertyValue = Long.parseLong(value);
                    }
                    break;
                case "int":
                case "integer":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(Integer::parseInt).collect(Collectors.toList());
                    } else {
                        propertyValue = Integer.parseInt(value);
                    }
                    break;
                case "double":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(Double::parseDouble).collect(Collectors.toList());
                    } else {
                        propertyValue = Double.parseDouble(value);
                    }
                    break;
                case "bool":
                case "boolean":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(Boolean::parseBoolean).collect(Collectors.toList());
                    } else {
                        propertyValue = Boolean.parseBoolean(value);
                    }
                    break;
                case "string":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).collect(Collectors.toList());
                    } else {
                        propertyValue = value;
                    }
                    break;
                default:
                    propertyName = header;
                    propertyValue = value;
            }
            return new AbstractMap.SimpleEntry<>(propertyName, propertyValue);
        }
    }
}
