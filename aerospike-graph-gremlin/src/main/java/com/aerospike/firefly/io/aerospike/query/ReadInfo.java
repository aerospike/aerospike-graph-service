package com.aerospike.firefly.io.aerospike.query;

import com.aerospike.client.exp.Expression;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.paged.VertexQueryHelper;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;

public class ReadInfo {
    public final String set;
    public final List<FireflyId> ids;
    public final Expression expression;
    public final List<String> requiredProperties;
    public final boolean areEdgesRequired;

    private ReadInfo(final Expression expression, final String set, final List<FireflyId> ids, final List<String> requiredProperties,
                     final boolean areEdgesRequired) {
        if (set == null) {
            // Should never happen.
            throw new IllegalArgumentException("Cannot construct ReadInfo with null set.");
        }
        if (ids == null) {
            // Should never happen.
            throw new IllegalArgumentException("Cannot construct ReadInfo with null ids.");
        }
        this.expression = expression;
        this.set = set;
        this.ids = ids;
        this.requiredProperties = requiredProperties;
        this.areEdgesRequired = areEdgesRequired;
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private Expression expression = null;
        private String set = null;
        private List<FireflyId> ids = null;
        private List<String> requiredProperties = null;
        private boolean areEdgesRequired = true;

        private Builder() {
        }

        public Builder exp(final Expression expression) {
            this.expression = expression;
            return this;
        }

        public Builder exp(final List<HasContainer> hasContainers, final AerospikeConnection db, final Class<? extends FireflyElement> clazz) {
            if (clazz.isAssignableFrom(FireflyVertex.class)) {
                this.expression = VertexQueryHelper.hasContainerListToExpression(db, hasContainers);
            } else {
                throw new IllegalArgumentException("ReadInfo.Builder.exp() only supports Vertex. Please contact support.");
            }
            return this;
        }

        public Builder set(final String set) {
            this.set = set;
            return this;
        }

        public Builder ids(final List<FireflyId> ids) {
            this.ids = ids;
            return this;
        }

        public Builder reqProps(final List<String> requiredProperties) {
            this.requiredProperties = requiredProperties;
            return this;
        }

        public Builder areEdgesRequired(final boolean areEdgesRequired) {
            this.areEdgesRequired = areEdgesRequired;
            return this;
        }

        public ReadInfo build() {
            return new ReadInfo(expression, set, ids, requiredProperties, areEdgesRequired);
        }
    }
}
