/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.stream.Collector;
import java.util.stream.Collectors;

public abstract class SparkFireflyElement implements Serializable {
    private static final String TYPE_DELIMITER = ":";
    private static final String MULTI_DELIMITER = ";";

    protected static final String SINGLE_CARDINALITY = "single";
    protected static final String LIST_CARDINALITY = "list";
    protected static final String SET_CARDINALITY = "set";

    private static final String LONG = "long";
    private static final String INT = "int";
    private static final String INTEGER = "integer";
    private static final String DOUBLE = "double";
    private static final String BOOL = "bool";
    private static final String BOOLEAN = "boolean";
    private static final String STRING = "string";
    private static final String DATE = "date";
    private static final String OFFSETDATETIME = "offsetdatetime";
    private static final String GEO = "geo";
    private static final Set<String> VALID_TYPES = Set.of(LONG, INT, INTEGER, DOUBLE, BOOL, BOOLEAN, STRING, DATE, OFFSETDATETIME, GEO);

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
                                                                final String nullValue,
                                                                final Set<String> validCardinalities) {
        final PropertyValueParser parser = new PropertyValueParser(nullValue);
        final Map<String, String> propertyInfo = getPropertyInfoFromHeader(header);
        final String cardinality = propertyInfo.get(PROPERTY_INFO_CARDINALITY);
        final String type = propertyInfo.get(PROPERTY_INFO_TYPE);
        final String propertyName = propertyInfo.get(PROPERTY_INFO_NAME);
        if (!validCardinalities.contains(cardinality)) {
            throw new InvalidCsvHeaderException(
                    String.format("Invalid cardinality '%s' detected in property header '%s'. " +
                            "Please refer to help documentation for supported cardinalities.", cardinality, header)
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
        final boolean isSingle = cardinality.equals(SINGLE_CARDINALITY);
        final boolean isList = cardinality.equals(LIST_CARDINALITY);
        final boolean isSet = cardinality.equals(SET_CARDINALITY);
        Collector collector = null;
        if (!isSingle) {
            if (isList) {
                collector = Collectors.toList();
            } else if (isSet) {
                collector = Collectors.toSet();
            } else {
                // This should never happen
                throw new IllegalStateException("No valid collector found for non-single cardinality. Please contact support.");
            }
        }
        final Object propertyValue;
        switch (type) {
            case LONG:
                if (isSingle) {
                    propertyValue = parser.parseLong(value);
                } else {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseLong).collect(collector);
                }
                break;
            case INT:
            case INTEGER:
                if (isSingle) {
                    propertyValue = parser.parseInt(value);
                } else {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseInt).collect(collector);
                }
                break;
            case DOUBLE:
                if (isSingle) {
                    propertyValue = parser.parseDouble(value);
                } else {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseDouble).collect(collector);
                }
                break;
            case BOOL:
            case BOOLEAN:
                if (isSingle) {
                    propertyValue = parser.parseBoolean(value);
                } else {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseBoolean).collect(collector);
                }
                break;
            case STRING:
                if (isSingle) {
                    propertyValue = parser.parseString(value);
                } else {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseString).collect(collector);
                }
                break;
            case DATE:
                if (isSingle) {
                    propertyValue = parser.parseDate(value);
                } else {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseDate).collect(collector);
                }
                break;
            case OFFSETDATETIME:
                if (isSingle) {
                    propertyValue = parser.parseOffsetDateTime(value);
                } else {
                    final String[] values = value.split(MULTI_DELIMITER);
                    propertyValue = Arrays.stream(values).map(parser::parseOffsetDateTime).collect(collector);
                }
                break;
            case GEO:
                if (isList) {
                    throw new InvalidCsvHeaderException(
                            String.format("Geo properties do not support list cardinality in header '%s'. Use set for multiple points.", header));
                }
                propertyValue = parser.parseGeo(value, isSet);
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
