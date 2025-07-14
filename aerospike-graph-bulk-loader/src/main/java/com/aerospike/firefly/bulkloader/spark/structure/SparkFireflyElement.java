package com.aerospike.firefly.bulkloader.spark.structure;

import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.bulkload.utils.exception.InvalidCsvHeaderException;
import com.aerospike.firefly.structure.id.FireflyId;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public abstract class SparkFireflyElement implements Serializable {
    private static final String TYPE_DELIMITER = ":";
    private static final String MULTI_DELIMITER = ";";

    protected static final String SINGLE_CARDINALITY = "single";
    protected static final String LIST_CARDINALITY = "list";
    protected static final String SET_CARDINALITY = "set";
    private static final Set<String> VALID_CARDINALITIES = Set.of(SINGLE_CARDINALITY, LIST_CARDINALITY);

    private static final String LONG = "long";
    private static final String INT = "int";
    private static final String INTEGER = "integer";
    private static final String DOUBLE = "double";
    private static final String BOOL = "bool";
    private static final String BOOLEAN = "boolean";
    private static final String STRING = "string";
    private static final String DATE = "date";
    private static final String OFFSETDATETIME = "offsetdatetime";
    private static final Set<String> VALID_TYPES = Set.of(LONG, INT, INTEGER, DOUBLE, BOOL, BOOLEAN, STRING, DATE, OFFSETDATETIME);

    protected static String PROPERTY_INFO_NAME = "PROPERTY_INFO_NAME";
    protected static String PROPERTY_INFO_TYPE = "PROPERTY_INFO_TYPE";
    protected static String PROPERTY_INFO_CARDINALITY = "PROPERTY_INFO_CARDINALITY";

    public static final String ID_HEADER = "~id";
    public static final String LABEL_HEADER = "~label";

    public final Object id;
    protected final String label;
    protected final List<Map.Entry<String, Object>> properties;

    protected SparkFireflyElement(final Object id, final String label,
                                  final List<Map.Entry<String, Object>> properties) {
        this.id = id;
        this.label = label;
        this.properties = properties;
    }

    public abstract FireflyId getFireflyId(final AerospikeConnection db);

    public Object getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public List<Map.Entry<String, Object>> getProperties() {
        return this.properties;
    }

    protected static Map.Entry<String, Object> generateProperty(final String header,
                                                                final String value,
                                                                final String nullValue) {
        final PropertyValueParser parser = new PropertyValueParser(nullValue);
        final Map<String, String> propertyInfo = getPropertyInfoFromHeader(header);
        final String cardinality = propertyInfo.get(PROPERTY_INFO_CARDINALITY);
        final String type = propertyInfo.get(PROPERTY_INFO_TYPE);
        final String propertyName = propertyInfo.get(PROPERTY_INFO_NAME);
        if (!VALID_CARDINALITIES.contains(cardinality)) {
            throw new InvalidCsvHeaderException(
                    String.format("Invalid cardinality '%s' detected in property header '%s'. " +
                                    "Cardinality must be of 'single' or 'list'.", cardinality, header)
            );
        }
        if (!VALID_TYPES.contains(type)) {
            throw new InvalidCsvHeaderException(
                    String.format("Invalid type '%s' detected in property header '%s'. " +
                            "Please refer to help documentation for supported types.", type, header)
            );
        }
        if (propertyName.isBlank()) {
            throw new InvalidCsvHeaderException(
                    String.format("Invalid property name '%s' detected in property header '%s'. " +
                            "Property name must not be blank.", propertyName, header)
            );
        }
        final boolean isList = cardinality.equals(LIST_CARDINALITY);
        final Object propertyValue;
        switch (type) {
            case LONG:
                if (isList) {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseLong).collect(Collectors.toList());
                } else {
                    propertyValue = parser.parseLong(value);
                }
                break;
            case INT:
            case INTEGER:
                if (isList) {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseInt).collect(Collectors.toList());
                } else {
                    propertyValue = parser.parseInt(value);
                }
                break;
            case DOUBLE:
                if (isList) {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseDouble).collect(Collectors.toList());
                } else {
                    propertyValue = parser.parseDouble(value);
                }
                break;
            case BOOL:
            case BOOLEAN:
                if (isList) {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseBoolean).collect(Collectors.toList());
                } else {
                    propertyValue = parser.parseBoolean(value);
                }
                break;
            case STRING:
                if (isList) {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseString).collect(Collectors.toList());
                } else {
                    propertyValue = parser.parseString(value);
                }
                break;
            case DATE:
                if (isList) {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseDate).collect(Collectors.toList());
                } else {
                    propertyValue = parser.parseDate(value);
                }
                break;
            case OFFSETDATETIME:
                if (isList) {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseOffsetDateTime).collect(Collectors.toList());
                } else {
                    propertyValue = parser.parseOffsetDateTime(value);
                }
                break;
            default:
                // This should never happen.
                throw new InvalidCsvHeaderException(
                        String.format("Unexpected problem when attempting to parse property header '%s'. " +
                                "Please contact support.", header)
                );
        }
        return new AbstractMap.SimpleEntry<>(propertyName, propertyValue);
    }

    static protected Map<String, String> getPropertyInfoFromHeader(final String header) {
        final Map<String, String> propertyInfo = new HashMap<>();
        final Stack<Integer> delimiterIndices = new Stack<>();
        final char[] headerArr = header.toCharArray();
        for (int i = 0; i < headerArr.length; i++) {
            if (String.valueOf(headerArr[i]).equals(TYPE_DELIMITER)) {
                delimiterIndices.push(i);
            }
        }
        String cardinality = SINGLE_CARDINALITY;
        String type = STRING;
        String propertyName = "";
        if (delimiterIndices.size() >= 2) {
            final int cardinalityIndex = delimiterIndices.pop();
            final int typeIndex = delimiterIndices.pop();
            cardinality = header.substring(cardinalityIndex + 1).toLowerCase();
            type = header.substring(typeIndex + 1, cardinalityIndex).toLowerCase();
            propertyName = header.substring(0, typeIndex);
        } else if (delimiterIndices.size() == 1) {
            final int typeIndex = delimiterIndices.pop();
            type = header.substring(typeIndex + 1).toLowerCase();
            propertyName = header.substring(0, typeIndex);
        } else {
            propertyName = header;
        }
        propertyInfo.put(PROPERTY_INFO_NAME, propertyName);
        propertyInfo.put(PROPERTY_INFO_TYPE, type);
        propertyInfo.put(PROPERTY_INFO_CARDINALITY, cardinality);
        return propertyInfo;
    }

    abstract protected boolean isVertexProperty(); // Need this later once set cardinality is supported
}
