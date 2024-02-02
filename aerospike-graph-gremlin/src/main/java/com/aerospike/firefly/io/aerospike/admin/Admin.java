package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.admin.services.AdminSindexServicePlugin;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Admin {

    public static void registerAdministrativeServices(final FireflyGraph firefly) {
        firefly.getServiceRegistry().registerService(new AdminSindexServicePlugin(firefly));
    }

    public static Map<UUID, Operation> operations = new ConcurrentHashMap<>();

    public static class Status {
        final Object[] results;
        final Code code;
        public final Operation op;

        public enum Code {
            RUNNING,
            COMPLETE,
            ERROR
        }

        public Status(Operation op, final Code code, final Object[] results) {
            this.results = results;
            this.code = code;
            this.op = op;
        }

        public static Status of(final Operation op, final Code code, final Object... results) {
            return new Status(op, code, Optional.ofNullable(results).orElse(new Object[]{}));
        }

        @Override
        public String toString() {
            final StringBuilder resultOutput = new StringBuilder();
            if (results.length != 0) {
                resultOutput.append("\n");
                for (final Object result : results) {
                    resultOutput
                            .append("\t")
                            .append(Optional.ofNullable(result).orElse("None"))
                            .append("\n");
                }
            }

            return String.format("AdminOperation:" + "\n" +
                            "UUID: %s" + "\n" +
                            "Code: %s" + "\n" +
                            "Results: %s",
                    op.uuid,
                    code,
                    resultOutput);
        }
    }


    public static class Operation implements Interface, Iterator<Status> {

        public final UUID uuid;
        private final FireflyGraph firefly;
        private final METHOD method;
        private final Future<Status> exe;
        private final Object[] args;
        private final AerospikeConnection db;
        private Object result;

        public Operation(final FireflyGraph firefly, final METHOD method, final Object[] args) {
            this.firefly = firefly;
            this.method = method;
            this.args = args;
            this.db = firefly.getBaseGraph();
            this.uuid = UUID.randomUUID();
            this.exe = run(firefly, this, method, args);
        }

        public static Operation create(final FireflyGraph firefly, final Interface.METHOD method, final Object... args) {
            final Operation op = new Operation(firefly, method, args);
            operations.put(op.uuid, op);
            return op;
        }

        public static CompletableFuture<Status> run(final FireflyGraph firefly, Operation op, final METHOD method, final Object... args) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    op.invoke();
                } catch (final Exception e) {
                    e.printStackTrace();
                    return Status.of(op, Status.Code.ERROR, e);
                }
                return Status.of(op, Status.Code.COMPLETE, op.result);
            });
        }

        private void invoke() {
            final Method invocationMethod;
            final Object result;
            try {
                invocationMethod = this.getClass().getMethod(this.method.name(), this.method.signature());
                result = invocationMethod.invoke(this, this.args);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
            this.result = result;
        }

        public Status status() {
            if (exe.isCancelled())
                return Status.of(this, Status.Code.ERROR, new RuntimeException("cancelled"));
            if (exe.isDone())
                return Status.of(this, Status.Code.COMPLETE, Optional.ofNullable(result));
            else
                return Status.of(this, Status.Code.RUNNING);
        }

        @Override
        public void createVertexPropertyIndex(final String key) {
            firefly.createIndexes(FireflyVertex.class, db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN, db.getVpIndexPrefix(), List.of(key));
        }

        @Override
        public void createAdjacencyIndex() {
            final List<String> existingIndexes = AerospikeConnection.getExistingIndexes(db);
            db.createAdjacencyIndex(existingIndexes);
        }

        @Override
        public void createVertexLabelIndex() {
            final List<String> existingIndexes = AerospikeConnection.getExistingIndexes(db);
            db.createVertexLabelIndex(existingIndexes);
        }

        @Override
        public List<String> listExistingIndexes() {
            return AerospikeConnection.getExistingIndexes(db);
        }

        @Override
        public Object getOperationResult(final String uuid) {
            return operations.get(UUID.fromString(uuid)).result;
        }

        @Override
        public boolean hasNext() {
            return !exe.isDone();
        }

        @Override
        public Status next() {
            try {
                return exe.get();
            } catch (Exception e) {
                return Status.of(this, Status.Code.ERROR, e);
            }
        }
    }

    public static class Pair {
        public static <K, V> Map.Entry<K, V> of(final K key, final V value) {
            return new AbstractMap.SimpleImmutableEntry<>(key, value);
        }
    }

    public interface Interface {
        class Keys {
            public static final String KEY = "key";
            public static final String UUID = "uuid";
        }

        enum METHOD {
            getOperationResult(Pair.of(Keys.UUID, String.class)),
            createVertexPropertyIndex(Pair.of(Keys.KEY, String.class)),
            createAdjacencyIndex(),
            createVertexLabelIndex(),
            listExistingIndexes();
            private final Map.Entry<String, Class<?>>[] signature;

            METHOD(final Map.Entry<String, Class<?>>... signature) {
                this.signature = signature;
            }

            public Class<?>[] signature() {
                return Arrays.stream(this.signature).map(Map.Entry::getValue).toArray(Class[]::new);
            }
        }

        void createVertexPropertyIndex(final String key);

        void createAdjacencyIndex();

        void createVertexLabelIndex();

        List<String> listExistingIndexes();

        Object getOperationResult(String uuid);
    }
}
