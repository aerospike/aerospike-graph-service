package com.aerospike.firefly.olap.process.packing;


import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DistributedAerospikeConnection {
    private final AerospikeConnection db;
    private final String namespace;
    private final String set;
    private final long packedElementCount;
    private final long packSize;
    private final HashFunction hashingFunction = Hashing.murmur3_128();

    private final String bin = "b";
    private final String secondBin = "b2";

    public DistributedAerospikeConnection(final AerospikeConnection db,
                                          final String namespace,
                                          final String set,
                                          final long packedElementCount,
                                          final long packSize) {
        this.db = db;
        this.namespace = namespace;
        this.set = set;
        this.packedElementCount = packedElementCount;
        this.packSize = packSize;
    }

    public DistributedAerospikeConnection(final AerospikeConnection db,
                                          final long packedElementCount,
                                          final long packSize) {
        this(db, db.getNamespace(), db.OLAP_SET, packedElementCount, packSize);
    }

    public DistributedAerospikeConnection(final FireflyGraph graph) {
        this(graph.getBaseGraph(), graph.fireflySummaryUpdater.getFireflyStatistics().totalVertexCount(), 10);
    }

    // Truncate OLAP set.

    public void truncateOlapSet() {
        db.truncate(null, db.OLAP_SET, null);
    }

    // Long accumulator functions.
    // TODO: Do we need set ?
    public void setAccumulator(final String name, final Long amount) {
        final Key key = new Key(namespace, set, name);
        final Bin ctr = new Bin(bin, amount);
        db.writeOperate(null, key, Operation.put(ctr));
    }

    public Long getAccumulatorLong(final String name) {
        final Key key = new Key(namespace, set, name);
        final Record record = db.writeOperate(null, key, Operation.get(bin));
        return record.getLong(bin);
    }

    public void addAccumulator(final String name, final Long amount) {
        final Key key = new Key(namespace, set, name);
        final Bin ctr = new Bin(bin, amount);
        db.writeOperate(null, key, Operation.add(ctr));
    }

    // Double accumulator functions.

    public void setAccumulator(final String name, final Double amount) {
        final Key key = new Key(namespace, set, name);
        final Bin ctr = new Bin(bin, amount);
        db.writeOperate(null, key, Operation.put(ctr));
    }

    public Double getAccumulatorDouble(final String name) {
        final Key key = new Key(namespace, set, name);
        final Record record = db.writeOperate(null, key, Operation.get(bin));
        return record.getDouble(bin);
    }

    public void addAccumulator(final String name, final Double amount) {
        final Key key = new Key(namespace, set, name);
        final Bin ctr = new Bin(bin, amount);
        db.writeOperate(null, key, Operation.add(ctr));
    }

    // Packing key function.

    Key getPackedKeyFromId(final Object id) {
        final long hashValue = hashingFunction.hashUnencodedChars(id.toString()).asLong() & Long.MAX_VALUE;
        final long numberOfPacks = (packedElementCount + packSize - 1) / packSize;
        final long packingId = hashValue % numberOfPacks;
        return new Key(namespace, set, packingId);
    }

    // Minimum value functions.

    public void setPackedMinValue(final String vertexId, final String value) {
        final Key key = getPackedKeyFromId(vertexId);
        final Expression filter = Exp.build(
                Exp.or(
                        Exp.not(Exp.binExists(bin)),
                        Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(vertexId), Exp.mapBin(bin))),
                        Exp.gt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(vertexId), Exp.mapBin(bin)), Exp.val(value))
                )
        );
        final Operation op = MapOperation.put(MapPolicy.Default, bin, Value.get(vertexId), Value.get(value));
        final WritePolicy policy = new WritePolicy();
        policy.filterExp = filter;
        db.writeOperate(policy, key, op);
    }

    public String getPackedMinValue(final String vertexId) {
        final Key key = getPackedKeyFromId(vertexId);
        final Operation op = MapOperation.getByKey(bin, Value.get(vertexId), MapReturnType.VALUE);
        final Record r = db.readOperate(null, key, op);
        if (r == null || !r.bins.containsKey(bin)) {
            return null;
        } else {
            return r.getString(bin);
        }
    }

    // Packed accumulator functions.

    public void addPackedAccumulatorDouble(final String vertexId, final Double amount, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        final String mapKeyId = vertexId + "_" + iteration;
        Operation incrememt = MapOperation.increment(MapPolicy.Default, bin, Value.get(mapKeyId), Value.get(amount));
        db.writeOperate(null, key, incrememt);
    }

    public Double getPackedAccumulatorDouble(final String vertexId, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        final String mapKeyId = vertexId + "_" + iteration;
        final Operation op = MapOperation.getByKey(bin, Value.get(mapKeyId), MapReturnType.VALUE);
        final Record r = db.readOperate(null, key, op);
        if (r == null) {
            throw new IllegalStateException("Record not found for key when getting packed value: " + key);
        } else if (!r.bins.containsKey(bin)) {
            return 0.0; // Not sure if we want to throw here.
        } else {
            return r.getDouble(bin);
        }
    }

    // pair operations

    public void addPair(final String vertexId, final String value1, final Double value2, final int iteration) {
        final Key key = new Key(namespace, set, vertexId + "_" + iteration);

        final ListPolicy policy = new ListPolicy();

        final Operation createOp = ListOperation.create(bin, ListOrder.UNORDERED, false);
        final Operation updateOp = ListOperation.append(policy, bin, Value.get(value1));

        final Operation createOp2 = ListOperation.create(secondBin, ListOrder.UNORDERED, false);
        final Operation updateOp2 = ListOperation.append(policy, secondBin, Value.get(value2));

        db.writeOperate(null, key, createOp, updateOp, createOp2, updateOp2);
    }

    public List<Pair<String, Double>> getPairs(final String vertexId, final int iteration) {
        final Key key = new Key(namespace, set, vertexId + "_" + iteration);
        final Record record = db.readOperate(null, key, Operation.get(bin), Operation.get(secondBin));
        if (record == null) {
            return Collections.emptyList();
        }

        final List<?> values1 = record.getList(bin);
        final List<?> values2 = record.getList(secondBin);

        if (values1 == null || values2 == null) {
            return Collections.emptyList();
        }

        final List<Pair<String, Double>> pairs = new ArrayList<>(values1.size());
        for (int i = 0; i < values1.size(); i++) {
            pairs.add(new Pair<>((String) values1.get(i), (Double) values2.get(i)));
        }

        return pairs;
    }

    // packed pair operations

    public void setPackedPairMax(final String vertexId, final String name, final Double value, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        final String mapKeyId = vertexId + "_" + iteration;

        final Expression filter = Exp.build(
                Exp.or(
                        Exp.not(Exp.binExists(bin)),
                        Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(mapKeyId), Exp.mapBin(bin))),
                        Exp.lt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT, Exp.val(mapKeyId), Exp.mapBin(secondBin)), Exp.val(value)),
                        Exp.and(
                                Exp.eq(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT, Exp.val(mapKeyId), Exp.mapBin(secondBin)),
                                        Exp.val(value)),
                                Exp.lt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(mapKeyId), Exp.mapBin(bin)),
                                        Exp.val(name))
                        )
                )
        );
        final Operation op1 = MapOperation.put(MapPolicy.Default, bin, Value.get(mapKeyId), Value.get(name));
        final Operation op2 = MapOperation.put(MapPolicy.Default, secondBin, Value.get(mapKeyId), Value.get(value));

        final WritePolicy policy = new WritePolicy();
        policy.filterExp = filter;
        db.writeOperate(policy, key, op1, op2);
    }

    public Pair<String, Double> getPackedPairMax(final String vertexId, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        final String mapKeyId = vertexId + "_" + iteration;

        final Operation op1 = MapOperation.getByKey(bin, Value.get(mapKeyId), MapReturnType.VALUE);
        final Operation op2 = MapOperation.getByKey(secondBin, Value.get(mapKeyId), MapReturnType.VALUE);

        final Record record = db.readOperate(null, key, op1, op2);
        if (record == null || !record.bins.containsKey(bin)) {
            return Pair.with(null, null);
        }

        return Pair.with(record.getString(bin), record.getDouble(secondBin));
    }

    public void addPackedPair(final String vertexId, final String name, final Double value, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        final String mapKeyId = vertexId + "_" + iteration;

        final Operation op1 = ListOperation.appendItems(
                ListPolicy.Default,
                bin,
                List.of(Value.get(name)),
                CTX.mapKeyCreate(Value.get(mapKeyId), MapOrder.KEY_ORDERED)
        );

        final Operation op2 = ListOperation.appendItems(
                ListPolicy.Default,
                secondBin,
                List.of(Value.get(value)),
                CTX.mapKeyCreate(Value.get(mapKeyId), MapOrder.KEY_ORDERED)
        );

        db.writeOperate(null, key, op1, op2);
    }

    public List<Pair<String, Double>> getPackedPairs(final String vertexId, final int iteration) {
        final String mapKeyId = vertexId + "_" + iteration;

        final Record record = getPackedRecord(vertexId, iteration);
        if (record == null || !record.bins.containsKey(bin) || !record.bins.containsKey(secondBin)) {
            return Collections.emptyList();
        }

        final List names = (List)record.getMap(bin).get(mapKeyId);
        final List values = (List)record.getMap(secondBin).get(mapKeyId);
        if (names == null || values == null || names.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Pair<String, Double>> pairs = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            pairs.add(new Pair<>((String) names.get(i), (Double) values.get(i)));
        }

        return pairs;
    }

    // For testing only.
    public Record getPackedRecord(final String vertexId, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        return db.read(key, null);
    }
}
