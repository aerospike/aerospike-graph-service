package com.aerospike.firefly.io.aerospike.query;

import com.aerospike.client.exp.Expression;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;

public class ReadInfo {
    public final String set;
    public final List<FireflyId> ids;
    public final Expression expression;
    public final List<String> requiredProperties;

    private ReadInfo(final Expression expression, final String set, final List<FireflyId> ids, final List<String> requiredProperties) {
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
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private Expression expression = null;
        private String set = null;
        private List<FireflyId> ids = null;
        private List<String> requiredProperties = null;

        private Builder() {
        }

        public Builder exp(final Expression expression) {
            this.expression = expression;
            return this;
        }

        public Builder exp(final List<HasContainer> hasContainers, final AerospikeConnection db, final Class<? extends FireflyElement> clazz) {
            this.expression = GraphQueryHelper.hasContainerListToExpression(db, hasContainers, clazz);
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

        public ReadInfo build() {
            return new ReadInfo(expression, set, ids, requiredProperties);
        }
    }
}
