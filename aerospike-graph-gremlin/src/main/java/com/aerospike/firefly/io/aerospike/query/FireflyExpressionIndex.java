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

package com.aerospike.firefly.io.aerospike.query;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;

public class FireflyExpressionIndex {
    static public final String LABEL_TOKEN = "~label";
    static private final String LABEL_KEY_MARKER = "L";
    static private final String WILDCARD_MARKER = "~*";
    static private final String NUMERIC_SEARCH_TOKEN = "~n*";
    static private final String STRING_SEARCH_TOKEN = "~s*";

    private final AerospikeConnection db;
    private final String prefix;
    private final List<Map.Entry<String, P<?>>> predicates;
    private final IndexType indexType;
    private final String searchKey;
    private String name = null;

    private FireflyExpressionIndex(final AerospikeConnection db, final String prefix,
                                   final List<Map.Entry<String, P<?>>> predicates, final IndexType indexType,
                                   final String searchKey) {
        this.db = db;
        this.prefix = prefix;
        this.predicates = predicates;
        this.indexType = indexType;
        this.searchKey = searchKey;
        if ((searchKey == null && indexType != IndexType.NUMERIC) ||
                (indexType != IndexType.NUMERIC && indexType != IndexType.STRING)) {
            // This should never happen
            throw new IllegalStateException("FireflyExpressionIndex was created with an invalid combination. Please contact support.");
        }
    }

    private FireflyExpressionIndex(final AerospikeConnection db, final String prefix,
                                   final List<Map.Entry<String, P<?>>> predicates) {
        this(db, prefix, predicates, IndexType.NUMERIC, null);
    }

    public String getName() {
        if (this.name == null) {
            final List<String> parts = new ArrayList<>();
            parts.add(prefix);

            // Build predicate strings
            final List<String> predicateStrings = new ArrayList<>();
            for (final Map.Entry<String, P<?>> entry : predicates) {
                final String key = entry.getKey();

                if (key.equals(LABEL_TOKEN)) {
                    // Label is always eq with string value, so just: L#{schemaValue}
                    final Long schemaValue = db.schemaManager.getVertexLabelWrite(entry.getValue().getValue().toString());
                    predicateStrings.add(LABEL_KEY_MARKER + "#" + schemaValue);
                } else {
                    // Regular property: {schemaKey}#{op}#{value}
                    final Long schemaKey = db.schemaManager.getVertexPropertyWrite(key);
                    final String op = entry.getValue().getBiPredicate().toString();
                    final Object value = entry.getValue().getValue();
                    predicateStrings.add(schemaKey + "#" + op + "#" + value);
                }
            }

            // Sort alphabetically for deterministic ordering
            predicateStrings.sort(String::compareTo);
            parts.addAll(predicateStrings);

            // Add search key wildcard if present
            if (searchKey != null) {
                final Long schemaSearchKey = db.schemaManager.getVertexPropertyWrite(searchKey);
                parts.add(schemaSearchKey + "#" + WILDCARD_MARKER);
            }

            // Add index type
            parts.add(indexType == IndexType.NUMERIC ? "N" : "S");

            this.name = String.join("|", parts);
        }
        return this.name;
    }

    public List<Map.Entry<String, P<?>>> getPredicates() {
        return List.copyOf(this.predicates);
    }

    public String getSearchKey() {
        return this.searchKey;
    }

    public IndexType getIndexType() {
        return this.indexType;
    }

    public int getWeight() {
        int weight = this.predicates.size();
        if (this.searchKey != null) {
            weight++;
        }
        return weight;
    }

    public Optional<FireflyIndexMetadata.ExpressionIndexInfo> getMatchingIndexInfo(final List<HasContainer> hasContainers) {
        if (hasContainers == null || hasContainers.size() < 2) {
            return Optional.empty();
        }
        final Map<HasContainer, Boolean> hasContainerUsageMap = new HashMap<>();
        for (final HasContainer hasContainer : hasContainers) {
            hasContainerUsageMap.put(hasContainer, false);
        }
        boolean searchKeyFound = this.searchKey == null;
        String searchValueString = null;
        Long searchValueNumeric = null;
        boolean allPredicatesFound = true;
        if (this.searchKey != null) {
            for (final HasContainer hasContainer : hasContainers) {
                final String key = hasContainer.getKey();
                final BiPredicate<?, ?> p = hasContainer.getBiPredicate();
                final Object value = hasContainer.getValue();
                if (key.equals(this.searchKey) && p.equals(Compare.eq)) {
                    if (this.indexType == IndexType.NUMERIC && (value instanceof Integer || value instanceof Long)) {
                        searchKeyFound = true;
                        if (value instanceof Integer) {
                            searchValueNumeric = ((Integer) value).longValue();
                        } else {
                            searchValueNumeric = (Long) value;
                        }
                        hasContainerUsageMap.put(hasContainer, true);
                        break;
                    } else if (this.indexType == IndexType.STRING && value instanceof String) {
                        searchKeyFound = true;
                        searchValueString = (String) value;
                        hasContainerUsageMap.put(hasContainer, true);
                        break;
                    }
                }
            }
        }
        for (final Map.Entry<String, P<?>> predicate : this.predicates) {
            boolean matchFound = false;
            final String key = predicate.getKey();
            final BiPredicate<?, ?> p = predicate.getValue().getBiPredicate();
            final Object value = predicate.getValue().getValue();
            final Iterator<HasContainer> hasContainerIterator = hasContainers.iterator();
            while (!matchFound && hasContainerIterator.hasNext()) {
                final HasContainer hasContainer = hasContainerIterator.next();
                if (key.equals(hasContainer.getKey())) {
                    final BiPredicate<?, ?> hasContainerP;
                    Object hasContainerValue = hasContainer.getValue();
                    Long hasContainerNumericValue = null;
                    if (hasContainerValue instanceof Long) {
                        hasContainerNumericValue = (Long) hasContainerValue;
                    } else if (hasContainerValue instanceof Integer) {
                        hasContainerNumericValue = ((Integer) hasContainerValue).longValue();
                        hasContainerValue = ((Integer) hasContainerValue).longValue();
                    }
                    if (hasContainer.getBiPredicate().equals(Compare.gt) && hasContainerNumericValue != null) {
                        hasContainerP = Compare.gte;
                        hasContainerValue = hasContainerNumericValue + 1;
                    } else if (hasContainer.getBiPredicate().equals(Compare.lt) && hasContainerNumericValue != null) {
                        hasContainerP = Compare.lte;
                        hasContainerValue = hasContainerNumericValue - 1;
                    } else {
                        hasContainerP = hasContainer.getBiPredicate();
                    }
                    if (p.equals(hasContainerP) && value.equals(hasContainerValue)) {
                        matchFound = true;
                        hasContainerUsageMap.put(hasContainer, true);
                    }
                }
            }
            if (!matchFound) {
                allPredicatesFound = false;
                break;
            }
        }
        if (searchKeyFound && allPredicatesFound) {
            final List<HasContainer> unusedHasContainers = new ArrayList<>();
            for (final Map.Entry<HasContainer, Boolean> hasContainerUsage : hasContainerUsageMap.entrySet()) {
                if (!hasContainerUsage.getValue()) {
                    unusedHasContainers.add(hasContainerUsage.getKey());
                }
            }
            return Optional.of(new FireflyIndexMetadata.ExpressionIndexInfo(this.getName(), searchValueString,
                    searchValueNumeric, unusedHasContainers));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FireflyExpressionIndex that = (FireflyExpressionIndex) o;
        return Objects.equals(getName(), that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName());
    }

    @Override
    public String toString() {
        return getName();
    }

    static public FireflyExpressionIndex fromIndexName(final AerospikeConnection db, final String indexName) {
        // Get the known prefix and strip it from the index name
        final String prefix = db.getVpExpressionIndexPrefix();
        final String expectedStart = prefix + "|";

        if (!indexName.startsWith(expectedStart)) {
            // This should never happen
            throw new IllegalArgumentException("Index name does not start with expected prefix: '" + prefix + "'. Please contact support.");
        }

        // Split prefix off for encoded index name
        final String encodedIndexInfo = indexName.substring(expectedStart.length());
        final String[] parts = encodedIndexInfo.split("\\|");

        if (parts.length < 2) {
            // This should never happen
            throw new IllegalArgumentException("Invalid index name format: '" + indexName + "'. Please contact support.");
        }

        // Last part is index type
        final String indexTypeStr = parts[parts.length - 1];
        final IndexType indexType = indexTypeStr.equals("N") ? IndexType.NUMERIC : IndexType.STRING;

        final List<Map.Entry<String, P<?>>> predicates = new ArrayList<>();
        String searchKey = null;

        // Parse parts (predicates and optional search key), excluding the last part (index type)
        for (int i = 0; i < parts.length - 1; i++) {
            final String part = parts[i];
            final String[] segments = part.split("#", 3);

            // Check if this is a wildcard search key ({schemaKey}#~*)
            if (segments.length == 2 && segments[1].equals(WILDCARD_MARKER)) {
                final Long schemaKey = Long.parseLong(segments[0]);
                searchKey = db.schemaManager.getVertexPropertyString(schemaKey);
                continue;
            }

            // Check if this is a label predicate (L#{schemaValue}) - always eq with string
            if (segments.length == 2 && segments[0].equals(LABEL_KEY_MARKER)) {
                final Long schemaValue = Long.parseLong(segments[1]);
                final String labelValue = db.schemaManager.getVertexLabelString(schemaValue);
                predicates.add(new AbstractMap.SimpleEntry<>(LABEL_TOKEN, P.eq(labelValue)));
                continue;
            }

            // Regular property predicate: {schemaKey}#{op}#{value}
            if (segments.length < 3) {
                // This should never happen
                throw new IllegalArgumentException("Invalid predicate format in index name: '" + part  + "'. Please contact support.");
            }

            final Long schemaKey = Long.parseLong(segments[0]);
            final String key = db.schemaManager.getVertexPropertyString(schemaKey);
            final P<?> predicate = createPredicate(segments[1], segments[2]);
            predicates.add(new AbstractMap.SimpleEntry<>(key, predicate));
        }

        final FireflyExpressionIndex index;
        if (searchKey != null) {
            index = new FireflyExpressionIndex(db, prefix, predicates, indexType, searchKey);
        } else {
            index = new FireflyExpressionIndex(db, prefix, predicates);
        }
        index.name = indexName;
        return index;
    }

    private static P<?> createPredicate(final String op, final String valueStr) {
        // Try to parse as number first
        final Object value;
        try {
            value = Long.parseLong(valueStr);
        } catch (final NumberFormatException e) {
            // Keep as string
            return createPredicate(op, (Object) valueStr);
        }
        return createPredicate(op, value);
    }

    private static P<?> createPredicate(final String op, final Object value) {
        switch (op) {
            case "eq":
                return P.eq(value);
            case "gte":
                if (value instanceof Long) {
                    return P.gte((Long) value);
                }
                throw new IllegalArgumentException("gte requires numeric value");
            case "lte":
                if (value instanceof Long) {
                    return P.lte((Long) value);
                }
                throw new IllegalArgumentException("lte requires numeric value");
            default:
                throw new IllegalArgumentException("Unknown operator: " + op);
        }
    }

    static public FireflyExpressionIndex fromConfigString(final AerospikeConnection db,
                                                          final String configString) {
        final String[] keyPredPairs = configString.split(",");
        if (keyPredPairs.length < 2) {
            throw new IllegalArgumentException("A compound index cannot be created with less than 2 search criteria.");
        }

        final List<Map.Entry<String, P<?>>> predicates = new ArrayList<>();
        IndexType indexType = null;
        String searchKey = null;

        for (final String keyPredPair : keyPredPairs) {
            final String[] keyPred = keyPredPair.split(":");
            if (keyPred.length != 2) {
                throw new IllegalArgumentException("The character ':' must appear only once as a delimiter in each search criteria for a compound index. Invalid criteria: " + keyPredPair);
            }
            final String key = keyPred[0];
            final String predicateString = keyPred[1];
            if (key.startsWith("~") && !key.equals(LABEL_TOKEN)) {
                throw new IllegalArgumentException("The character '~' denoting reserved keys for compound indexes cannot be used except for '" + LABEL_TOKEN + "'.");
            }

            // Check for wildcard search token (~n* for numeric, ~s* for string)
            if (predicateString.equals(NUMERIC_SEARCH_TOKEN) || predicateString.equals(STRING_SEARCH_TOKEN)) {
                if (key.equals(LABEL_TOKEN)) {
                    throw new IllegalArgumentException("'" + LABEL_TOKEN + "' cannot be used as a search key.");
                }
                if (searchKey != null) {
                    throw new IllegalArgumentException("Only one search key with type indicator (" + NUMERIC_SEARCH_TOKEN + " or " + STRING_SEARCH_TOKEN + ") is allowed per compound index.");
                }
                indexType = predicateString.equals(NUMERIC_SEARCH_TOKEN) ? IndexType.NUMERIC : IndexType.STRING;
                searchKey = key;
                continue;
            }

            // Label is always eq with string value
            if (key.equals(LABEL_TOKEN)) {
                predicates.add(new AbstractMap.SimpleEntry<>(key, P.eq(predicateString)));
                continue;
            }

            final P<?> predicate = pFromString(predicateString);
            predicates.add(new AbstractMap.SimpleEntry<>(key, predicate));
        }

        if (searchKey != null) {
            return new FireflyExpressionIndex(db, db.getVpExpressionIndexPrefix(), predicates, indexType, searchKey);
        } else {
            return new FireflyExpressionIndex(db, db.getVpExpressionIndexPrefix(), predicates);
        }
    }

    static private P<?> pFromString(final String predicateString) {
        if (!predicateString.startsWith("~")) {
            // If value does not start with ~, just treat the entire value as a P.eq
            try {
                final long value = Long.parseLong(predicateString);
                return P.eq(value);
            } catch (final NumberFormatException e) {
                return P.eq(predicateString);
            }
        }

        // Remove ~ to parse
        final String tokenlessPredicate = predicateString.substring(1);

        final int openParen = tokenlessPredicate.indexOf('(');
        final int closeParen = tokenlessPredicate.lastIndexOf(')');

        if (openParen == -1 || closeParen == -1 || closeParen != tokenlessPredicate.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid predicate format. Expected format: ~operator(value), got: " + predicateString);
        }

        final String operator = tokenlessPredicate.substring(0, openParen).toLowerCase();
        final String valueString = tokenlessPredicate.substring(openParen + 1, closeParen);

        // Eq allows both string and number
        if (operator.equals("eq")) {
            try {
                final long value = Long.parseLong(valueString);
                return P.eq(value);
            } catch (final NumberFormatException e) {
                return P.eq(valueString);
            }
        }

        // For gt, gte, lt, and lte, value must be a number
        final long value;
        try {
            value = Long.parseLong(valueString);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Predicate '" + operator + "' requires a number value, got: " + valueString);
        }

        // Convert gt/lt to gte/lte with offset of 1
        switch (operator) {
            case "gt":
                return P.gte(value + 1);
            case "gte":
                return P.gte(value);
            case "lt":
                return P.lte(value - 1);
            case "lte":
                return P.lte(value);
            default:
                throw new IllegalArgumentException(
                        "Unknown predicate operator: " + operator + ". Valid operators are: eq, gt, gte, lt, lte");
        }
    }
}
