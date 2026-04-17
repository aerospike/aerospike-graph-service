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

package com.aerospike.firefly.features;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.UUID;

abstract class FireflyElementFeatures implements Graph.Features.ElementFeatures {
    /**
     * Determines if an {@link Element} allows properties with {@code null} property values. In the event that
     * this value is {@code false}, the underlying graph must treat {@code null} as an indication to remove
     * the property.
     */
    @Override
    public boolean supportsNullPropertyValues() {
        return false;
    }

    /**
     * Determines if an {@link Element} allows properties to be added.  This feature is set independently from
     * supporting "data types" and refers to support of calls to {@link Element#property(String, Object)}.
     */
    @Override
    public boolean supportsAddProperty() {
        return true;
    }

    /**
     * Determines if an {@link Element} allows properties to be removed.
     */
    @Override
    public boolean supportsRemoveProperty() {
        return true;
    }

    /**
     * Determines if an {@link Element} can have a user defined identifier.  Implementations that do not support
     * this feature will be expected to auto-generate unique identifiers.  In other words, if the {@link Graph}
     * allows {@code graph.addVertex(id,x)} to work and thus set the identifier of the newly added
     * {@link Vertex} to the value of {@code x} then this feature should return true.  In this case, {@code x}
     * is assumed to be an identifier data type that the {@link Graph} will accept.
     */
    @Override
    abstract public boolean supportsUserSuppliedIds();

    /**
     * Determines if an {@link Element} has numeric identifiers as their internal representation. In other
     * words, if the value returned from {@link Element#id()} is a numeric value then this method
     * should be return {@code true}.
     * <p/>
     * Note that this feature is most generally used for determining the appropriate tests to execute in the
     * Gremlin Test Suite.
     */
    @Override
    abstract public boolean supportsNumericIds();

    /**
     * Determines if an {@link Element} has string identifiers as their internal representation. In other
     * words, if the value returned from {@link Element#id()} is a string value then this method
     * should be return {@code true}.
     * <p/>
     * Note that this feature is most generally used for determining the appropriate tests to execute in the
     * Gremlin Test Suite.
     */
    @Override
    public boolean supportsStringIds() {
        return true;
    };

    /**
     * Determines if an {@link Element} has UUID identifiers as their internal representation. In other
     * words, if the value returned from {@link Element#id()} is a {@link UUID} value then this method
     * should be return {@code true}.
     * <p/>
     * Note that this feature is most generally used for determining the appropriate tests to execute in the
     * Gremlin Test Suite.
     */
    @Override
    public boolean supportsUuidIds() {
        return false;
    }

    /**
     * Determines if an {@link Element} has a specific custom object as their internal representation.
     * In other words, if the value returned from {@link Element#id()} is a type defined by the graph
     * implementations, such as OrientDB's {@code Rid}, then this method should be return {@code true}.
     * <p/>
     * Note that this feature is most generally used for determining the appropriate tests to execute in the
     * Gremlin Test Suite.
     */
    @Override
    public boolean supportsCustomIds() {
        return false;
    }

    /**
     * Determines if an {@link Element} any Java object is a suitable identifier. TinkerGraph is a good
     * example of a {@link Graph} that can support this feature, as it can use any {@link Object} as
     * a value for the identifier.
     * <p/>
     * Note that this feature is most generally used for determining the appropriate tests to execute in the
     * Gremlin Test Suite. This setting should only return {@code true} if {@link #supportsUserSuppliedIds()}
     * is {@code true}.
     */
    @Override
    public boolean supportsAnyIds() {
        return false;
    }

    /**
     * Determines if an identifier will be accepted by the {@link Graph}.  This check is different than
     * what identifier internally supports as defined in methods like {@link #supportsNumericIds()}.  Those
     * refer to internal representation of the identifier.  A {@link Graph} may accept an identifier that
     * is not of those types and internally transform it to a native representation.
     * <p/>
     * Note that this method only applies if {@link #supportsUserSuppliedIds()} is {@code true}. Those that
     * return {@code false} for that method can immediately return false for this one as it allows no ids
     * of any type (it generates them all).
     * <p/>
     * The default implementation will immediately return {@code false} if {@link #supportsUserSuppliedIds()}
     * is {@code false}.  If custom identifiers are supported then it will throw an exception.  Those that
     * return {@code true} for {@link #supportsCustomIds()} should override this method. If
     * {@link #supportsAnyIds()} is {@code true} then the identifier will immediately be allowed.  Finally,
     * if any of the other types are supported, they will be typed checked against the class of the supplied
     * identifier.
     */
    @Override
    public boolean willAllowId(final Object id) {
        if (!supportsUserSuppliedIds()) return false;
        if (supportsCustomIds())
            throw new UnsupportedOperationException("The default implementation is not capable of validating custom ids - please override");

        return supportsAnyIds() || (supportsStringIds() && id instanceof String)
                || (supportsNumericIds() && id instanceof Number) || (supportsUuidIds() && id instanceof UUID);
    }
}
