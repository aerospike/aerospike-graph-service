package com.aerospike.firefly.structure;

import com.aerospike.firefly.structure.id.IdManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyGraphFeatures implements Graph.Features {

    private static final boolean USER_SUPPLIED_IDS = true;
    private final FireflyGraph fireflyGraph;
    private final FireflyEdgeFeatures edgeFeatures;
    private final FireflyVertexFeatures vertexFeatures;
    private final FireflyVertexPropertyFeatures vertexPropertyFeatures;

    private final FireflyGraphGraphFeatures graphFeatures;


    FireflyGraphFeatures(FireflyGraph fireflyGraph) {
        this.fireflyGraph = fireflyGraph;
        edgeFeatures = new FireflyEdgeFeatures(fireflyGraph.edgeIdManager);
        vertexFeatures = new FireflyVertexFeatures(fireflyGraph.vertexIdManager);
        vertexPropertyFeatures = new FireflyVertexPropertyFeatures(fireflyGraph.vertexPropertyIdManager);
        graphFeatures = new FireflyGraphGraphFeatures();
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }


    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }


    public class FireflyGraphGraphFeatures implements Graph.Features.GraphFeatures {

        private FireflyGraphGraphFeatures() {
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public boolean supportsOrderabilitySemantics() {
            return false;
        }

        @Override
        public boolean supportsServiceCall() {
            return false;
        }

        @Override
        public VariableFeatures variables() {
            return new FireflyVariableFeatures();
        }

        public class FireflyVariableFeatures implements VariableFeatures {
            String FEATURE_VARIABLES = "Variables";

            @Override
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            public boolean supportsByteArrayValues() {
                return true;
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringValues() {
                return true;
            }

            @Override
            public boolean supportsIntegerValues() {
                return true;
            }

            @Override
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }

            @Override
            public boolean supportsVariables() {
                return supportsBooleanValues() || supportsByteValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMixedListValues() || supportsSerializableValues()
                        || supportsStringValues() || supportsUniformListValues() || supportsBooleanArrayValues()
                        || supportsByteArrayValues() || supportsDoubleArrayValues() || supportsFloatArrayValues()
                        || supportsIntegerArrayValues() || supportsLongArrayValues() || supportsStringArrayValues();
            }
        }


    }

    public class FireflyVertexFeatures implements Graph.Features.VertexFeatures {
        private final IdManager vertexIdManager;

        public FireflyVertexFeatures(IdManager vertexIdManager) {
            this.vertexIdManager = vertexIdManager;
        }

        private final FireflyVertexPropertyFeatures vertexPropertyFeatures = new FireflyVertexPropertyFeatures(fireflyGraph.vertexPropertyIdManager);


        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }

        @Override
        public VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }
        @Override
        public boolean supportsMultiProperties() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }


        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return FireflyGraphFeatures.USER_SUPPLIED_IDS;
        }

        @Override
        public boolean supportsStringIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }


        @Override
        public boolean willAllowId(final Object id) {
            return supportsUserSuppliedIds() && vertexIdManager.allow(id.getClass());
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }
    }

    public class FireflyEdgeFeatures implements Graph.Features.EdgeFeatures {

        private final Graph.Features.EdgePropertyFeatures edgePropertyFeatures = new FireflyEdgePropertyFeatures();
        private final IdManager<?> edgeIdManager;

        private FireflyEdgeFeatures(IdManager<?> edgeIdManager) {
            this.edgeIdManager = edgeIdManager;
        }

        @Override
        public Graph.Features.EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return FireflyGraphFeatures.USER_SUPPLIED_IDS;
        }

        @Override
        public boolean supportsStringIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }


        @Override
        public boolean willAllowId(final Object id) {
            return supportsUserSuppliedIds() && edgeIdManager.allow(id.getClass());
        }
    }

    public static class FireflyEdgePropertyFeatures implements Graph.Features.EdgePropertyFeatures {

        FireflyEdgePropertyFeatures() {
        }


        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return true;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return true;
        }

        @Override
        public boolean supportsIntegerValues() {
            return true;
        }

        @Override
        public boolean supportsFloatValues() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }

    public class FireflyVertexPropertyFeatures implements Graph.Features.VertexPropertyFeatures {

        private final IdManager vertexPropertyIdManager;

        private FireflyVertexPropertyFeatures(IdManager vertexPropertyIdManager) {
            this.vertexPropertyIdManager = vertexPropertyIdManager;
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return FireflyGraphFeatures.USER_SUPPLIED_IDS;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return true;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }


        //@todo
        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return true;
        }
        @Override
        public boolean supportsStringIds() {
            return false;
        }

        @Override
        public boolean supportsIntegerValues() {
            return true;
        }

        @Override
        public boolean supportsFloatValues() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return supportsUserSuppliedIds() && vertexPropertyIdManager.allow(id.getClass());
        }
    }
}
