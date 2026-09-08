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

package com.aerospike.firefly.olap.process.packing;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.QueryDuration;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.AerospikeConnection.FireflyRecordSet;
import com.aerospike.firefly.olap.structure.job.Job;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.FireflyGeoValue;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.olap.process.ConnectedComponentProgram.VertexStatus;
import static com.aerospike.firefly.util.FireflyHelper.isGeoVertexPropertyValue;
import static com.aerospike.firefly.util.FireflyHelper.validateAndConvertVertexPropertyValue;

public class DistributedAerospikeConnection {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedAerospikeConnection.class);

    private static final Set<Integer> RETRYABLE_CODES = Set.of(
            ResultCode.DEVICE_OVERLOAD,
            ResultCode.KEY_BUSY,
            ResultCode.QUERY_TIMEOUT,
            ResultCode.TIMEOUT,
            ResultCode.NO_MORE_CONNECTIONS,
            ResultCode.INVALID_NODE_ERROR,
            ResultCode.BATCH_FAILED
    );

    private final FireflyGraph graph;
    private final AerospikeConnection db;
    private final String namespace;
    // used for temporary run-time data
    private final String tempSet;
    private final String jobSet;
    private final long packedElementCount;
    private final long packSize;
    private final HashFunction hashingFunction = Hashing.murmur3_128();

    private final String bin = "b";
    private final String secondBin = "b2";

    private final String jobId;
    private final boolean isAlgorithm;

    public DistributedAerospikeConnection(final FireflyGraph graph,
                                          final String namespace,
                                          final String tempSet,
                                          final String jobId,
                                          final boolean isAlgorithm,
                                          final long packedElementCount,
                                          final long packSize) {
        this.graph = graph;
        this.db = graph.getBaseGraph();
        this.namespace = namespace;
        this.tempSet = tempSet;
        this.jobId = jobId;
        this.isAlgorithm = isAlgorithm;
        this.packedElementCount = packedElementCount;
        this.packSize = packSize;

        this.jobSet = graph.getBaseGraph().getConfig().olapJobSet;

        createIndexes();
    }

    public DistributedAerospikeConnection(final FireflyGraph graph,
                                          final String jobId,
                                          final long packedElementCount,
                                          final long packSize,
                                          final boolean isAlgorithm) {
        this(graph, graph.getBaseGraph().getNamespace(),
                isAlgorithm ? graph.getBaseGraph().getConfig().olapAlgorithmTempSet : graph.getBaseGraph().getConfig().olapTempSet, jobId, isAlgorithm, packedElementCount, packSize);
    }

    public DistributedAerospikeConnection(final FireflyGraph graph,
                                          final long packedElementCount,
                                          final long packSize,
                                          final boolean isAlgorithm) {
        this(graph, graph.getBaseGraph().getNamespace(), graph.getBaseGraph().getConfig().olapAlgorithmTempSet, null, isAlgorithm, packedElementCount, packSize);

        if (!isAlgorithm) {
            throw new IllegalArgumentException("jobId is required, use different constructor");
        }
    }

    public DistributedAerospikeConnection(final FireflyGraph graph, final String jobId) {
        this(graph, jobId, graph.fireflySummaryUpdater.getFireflyStatistics().totalVertexCount(), 10, jobId == null);
    }

    public DistributedAerospikeConnection(final FireflyGraph graph, final boolean isAlgorithm) {
        this(graph, graph.fireflySummaryUpdater.getFireflyStatistics().totalVertexCount(), 10, isAlgorithm);
    }

    private void createIndexes() {
        final List<String> existingIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());

        // Create index on job state if not exists.
        db.createIndex(existingIndexes, jobSet, db.getConfig().graphId + "_job_state_IDX", "state", IndexType.STRING, IndexCollectionType.DEFAULT);
        AerospikeConnection.InfoOps.createSetIndex(db, jobSet);
    }

    // Truncate OLAP set.
    public void truncateOlapTempSet() {
        db.truncate(null, tempSet, null);
    }

    public void deleteTemporaryData() {
        final Key key = new Key(namespace, tempSet, jobId);
        db.delete(key, null, true);
    }

    // Algorithm use separate set, so name can be used as record key.
    // Aggregate queries use single record, so name can be used as bin.
    Key getKey(final String name) {
        return isAlgorithm ? new Key(namespace, tempSet, name) : new Key(namespace, tempSet, jobId);
    }

    String getBinName(final String name) {
        // bin name limit is 15 chars
        return isAlgorithm ? bin : (name.length() > 15 ? name.substring(0, 15) : name);
    }

    private WritePolicy getWritePolicy() {
        // algorithm should care about own temporary data.
        if (isAlgorithm)
            return null;

        final WritePolicy policy = new WritePolicy();
        policy.expiration = 7 * 24 * 60 * 60; // 7 days
        return policy;
    }

    // Long accumulator functions.
    public void setAccumulator(final String name, final Long amount) {
        final Bin ctr = new Bin(getBinName(name), amount);
        db.writeOperate(getWritePolicy(), getKey(name), Operation.put(ctr));
    }

    public Long getAccumulatorLong(final String name) {
        final Record record = db.writeOperate(null, getKey(name), Operation.get(getBinName(name)));
        return record.getLong(getBinName(name));
    }

    public void addAccumulator(final String name, final Long amount) {
        final Bin ctr = new Bin(getBinName(name), amount);
        db.writeOperate(getWritePolicy(), getKey(name), Operation.add(ctr));
    }

    // Double accumulator functions.

    public void setAccumulator(final String name, final Double amount) {
        final Bin ctr = new Bin(getBinName(name), amount);
        db.writeOperate(getWritePolicy(), getKey(name), Operation.put(ctr));
    }

    public Double getAccumulatorDouble(final String name) {
        final Record record = db.writeOperate(null, getKey(name), Operation.get(getBinName(name)));
        return record.getDouble(getBinName(name));
    }

    public void addAccumulator(final String name, final Double amount) {
        final Bin ctr = new Bin(getBinName(name), amount);
        db.writeOperate(getWritePolicy(), getKey(name), Operation.add(ctr));
    }

    /////////////// packing function used only for algorithms

    // Packing key function.

    Key getPackedKeyFromId(final Object id) {
        final long hashValue = hashingFunction.hashUnencodedChars(id.toString()).asLong() & Long.MAX_VALUE;
        final long numberOfPacks = (packedElementCount + packSize - 1) / packSize;
        final long packingId = hashValue % numberOfPacks;
        return new Key(namespace, tempSet, packingId);
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
    public void setPackedAccumulatorDouble(final String vertexId, final Double value, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        final String mapKeyId = vertexId + "_" + iteration;
        final Operation put = MapOperation.put(MapPolicy.Default, bin, Value.get(mapKeyId), Value.get(value));
        db.writeOperate(null, key, put);
    }

    public void addPackedAccumulatorDouble(final String vertexId, final Double amount, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        final String mapKeyId = vertexId + "_" + iteration;
        Operation increment = MapOperation.increment(MapPolicy.Default, bin, Value.get(mapKeyId), Value.get(amount));
        db.writeOperate(null, key, increment);
    }

    public Double getPackedAccumulatorDoubleSum(final List<String> vertexIds, final int iteration) {
        if (vertexIds.isEmpty()) {
            return 0.0;
        }

        final Key[] keys = vertexIds.stream().map(this::getPackedKeyFromId).toArray(Key[]::new);
        final Record[] records = db.dynamicBatchRead(keys, null, null);

        double sum = 0.0;
        for (int i = 0; i < vertexIds.size(); i++) {
            final String mapKeyId = vertexIds.get(i) + "_" + iteration;
            sum += (double) records[i].getMap(bin).get(mapKeyId);
        }

        return sum;
    }

    public void setPackedAccumulatorDouble(final List<Pair<byte[], Double>> vertexEnergy, final int iteration) {
        if (vertexEnergy.isEmpty()) {
            return;
        }

        final List<BatchRecord> batchRecords = new ArrayList<>();
        for (final Pair<byte[], Double> entry : vertexEnergy) {
            final Key key = getPackedKeyFromId(toString(entry.getValue0()));
            final String mapKeyId = toString(entry.getValue0());
            final Operation put = MapOperation.put(MapPolicy.Default, bin + "_" + iteration, Value.get(mapKeyId), Value.get(entry.getValue1()));
            batchRecords.add(new BatchWrite(key, new Operation[]{put}));
        }

        writeWithBackoff(batchRecords);
    }

    private static int allowedErrorPrints = 3;

    public Map<ByteArrayWrapper, Double> getPackedAccumulatorDoubleCache(final List<ByteArrayWrapper> vertexIds, final int iteration) {
        if (vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        final String binName = bin + "_" + iteration;
        final int chunkSize = db.getConfig().paginationPageSize;
        final Map<ByteArrayWrapper, Double> result = new HashMap<>();
        for (int i = 0; i < vertexIds.size(); i += chunkSize) {
            final int end = Math.min(vertexIds.size(), i + chunkSize);
            final List<ByteArrayWrapper> chunk = vertexIds.subList(i, end);

            final Key[] keys = chunk.stream().map(k -> getPackedKeyFromId(toString(k.getData()))).toArray(Key[]::new);
            final Operation op = Operation.get(binName);

            final Record[] records = db.dynamicBatchRead(keys, null, null, op);

            for (int j = 0; j < chunk.size(); j++) {
                final String mapKeyId = toString(chunk.get(j).getData());
                try {
                    result.put(chunk.get(j), (double) records[j].getMap(binName).get(mapKeyId));
                } catch (final Exception e) {
                    if (--allowedErrorPrints > 0) {
                        System.out.println("getPackedAccumulatorDoubleCache failed for " + mapKeyId + "; record map " + records[j].getMap(binName));
                    }
                    result.put(chunk.get(j), 0.0);
                }
            }
        }

        return result;
    }

    private String toString(byte[] arr) {
        StringBuilder sb = new StringBuilder(arr.length * 2);
        for (final byte b : arr) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
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
        final Key key = new Key(namespace, tempSet, vertexId + "_" + iteration);

        final ListPolicy policy = new ListPolicy();

        final Operation createOp = ListOperation.create(bin, ListOrder.UNORDERED, false);
        final Operation updateOp = ListOperation.append(policy, bin, Value.get(value1));

        final Operation createOp2 = ListOperation.create(secondBin, ListOrder.UNORDERED, false);
        final Operation updateOp2 = ListOperation.append(policy, secondBin, Value.get(value2));

        db.writeOperate(null, key, createOp, updateOp, createOp2, updateOp2);
    }

    public List<Pair<String, Double>> getPairs(final String vertexId, final int iteration) {
        final Key key = new Key(namespace, tempSet, vertexId + "_" + iteration);
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

        final List names = (List) record.getMap(bin).get(mapKeyId);
        final List values = (List) record.getMap(secondBin).get(mapKeyId);
        if (names == null || values == null || names.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Pair<String, Double>> pairs = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            pairs.add(new Pair<>((String) names.get(i), (Double) values.get(i)));
        }

        return pairs;
    }

    //////////////////// Connected component V2
    // one-character bin names to reduce storage
    private static final String RECORD_ID_BIN = "r";
    private static final String OTHER_RECORD_IDS_BIN = "o";
    private static final String MERGED_BIN = "m";
    private static final String GROUP_ID_BIN = "g";
    private static final String VERTEX_STATUS_BIN = "v";
    public static final int WRITE_PAGE_SIZE = 100;

    public void writeVertex(final String vertexId) {
        final Key key = new Key(namespace, tempSet, vertexId);

        db.checkedPut(null, key, new Bin(VERTEX_STATUS_BIN, VertexStatus.UNVISITED.ordinal()));
    }

    // return status and current recordId
    public Pair<VertexStatus, String> visitVertex(final String vertexId,
                                                  final String recordId,
                                                  final VertexStatus newStatus) {
        final Key key = new Key(namespace, tempSet, vertexId);

        final Operation readRecordIdOp = Operation.get(RECORD_ID_BIN);
        final Operation readStatusOp = Operation.get(VERTEX_STATUS_BIN);
        final Operation writeOp = ExpOperation.write(RECORD_ID_BIN, Exp.build(Exp.val(recordId)),
                ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL);

        final Expression updateStatusExp = Exp.build(
                Exp.cond(
                        Exp.eq(Exp.intBin(VERTEX_STATUS_BIN), Exp.val(VertexStatus.UNVISITED.ordinal())),
                        Exp.val(newStatus.ordinal()),
                        Exp.intBin(VERTEX_STATUS_BIN)
                )
        );
        final Operation writeStatusOp = newStatus == VertexStatus.COMPLETED
                // always allow setting to complete
                ? Operation.put(new Bin(VERTEX_STATUS_BIN, VertexStatus.COMPLETED.ordinal()))
                // visited can be set only if current status is unvisited
                : ExpOperation.write(VERTEX_STATUS_BIN, updateStatusExp, ExpWriteFlags.DEFAULT);

        final Record record = (Record) tryWithBackoff(() -> db.read(key, null,
                new Operation[]{readRecordIdOp, readStatusOp, writeOp, writeStatusOp}));

        return Pair.with(VertexStatus.values()[((Long) record.getList(VERTEX_STATUS_BIN).get(0)).intValue()],
                (String) record.getList(RECORD_ID_BIN).get(0));
    }

    public void markCompleted(final List<String> vertexIds) {
        if (vertexIds.isEmpty()) {
            return;
        }

        final BatchWritePolicy writePolicy = new BatchWritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        final Operation opSetCompleted = Operation.put(new Bin(VERTEX_STATUS_BIN, VertexStatus.COMPLETED.ordinal()));

        final List<BatchRecord> batchRecords = vertexIds.stream()
                .map(vertexId -> new BatchWrite(writePolicy, new Key(namespace, tempSet, vertexId), new Operation[]{opSetCompleted}))
                .collect(Collectors.toList());

        writeWithPaging(batchRecords);
    }

    public Map<Object, String> readVertexGroupRecordId(final List<Object> vertexIds) {
        final Key[] keys = vertexIds.stream().map(id -> new Key(namespace, tempSet, id.toString())).toArray(Key[]::new);
        final Record[] records = db.dynamicBatchRead(keys, null, null);

        final Map<Object, String> result = new HashMap<>();
        for (int i = 0; i < vertexIds.size(); i++) {
            // record should always exist
            result.put(vertexIds.get(i), records[i].getString(RECORD_ID_BIN));
        }
        return result;
    }

    public void createGroup(final String recordId, final String groupId) {
        final Key key = new Key(namespace, tempSet, recordId);

        db.checkedPut(null, key, new Bin(GROUP_ID_BIN, groupId));
    }

    public void linkGroups(final Set<String> recordIds, final String otherRecordId) {
        if (recordIds.isEmpty()) {
            return;
        }

        final ListPolicy listPolicy = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
        final Operation opLink = ListOperation.append(listPolicy, OTHER_RECORD_IDS_BIN, Value.get(otherRecordId));
        final BatchWritePolicy writePolicy = new BatchWritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        final List<BatchRecord> batchRecords = recordIds.stream()
                .map(recordId -> new BatchWrite(writePolicy, new Key(namespace, tempSet, recordId), new Operation[]{opLink}))
                .collect(Collectors.toList());

        // link back
        final Operation opLinkBack = ListOperation.appendItems(listPolicy, OTHER_RECORD_IDS_BIN,
                recordIds.stream().map(Value::get).collect(Collectors.toList()));
        batchRecords.add(new BatchWrite(writePolicy, new Key(namespace, tempSet, otherRecordId), new Operation[]{opLinkBack}));

        writeWithPaging(batchRecords);
    }

    public void finalizeGroups(final Set<String> recordIds, final String groupId) {
        final BatchWritePolicy writePolicy = new BatchWritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        final Operation opSetMerged = Operation.put(new Bin(MERGED_BIN, Value.get(true)));
        final Operation opSetGroup = Operation.put(new Bin(GROUP_ID_BIN, Value.get(groupId)));

        final List<BatchRecord> batchRecords = recordIds.stream()
                .map(recordId -> new BatchWrite(writePolicy, new Key(namespace, tempSet, recordId), new Operation[]{opSetMerged, opSetGroup}))
                .collect(Collectors.toList());

        writeWithPaging(batchRecords);
    }

    public Pair<String, List<String>> readGroup(final String recordId) {
        final Key key = new Key(namespace, tempSet, recordId);

        final Record record = db.read(key, null);
        if (record.getBoolean(MERGED_BIN)) {
            return Pair.with(record.getString(GROUP_ID_BIN), null);
        }

        return Pair.with(record.getString(GROUP_ID_BIN), (List<String>) record.getList(OTHER_RECORD_IDS_BIN));
    }

    // to be used in tests only
    public Pair<String, List<String>> readGroupDirty(final String recordId) {
        final Key key = new Key(namespace, tempSet, recordId);

        final Record record = db.read(key, null);

        return Pair.with(record.getString(GROUP_ID_BIN), (List<String>) record.getList(OTHER_RECORD_IDS_BIN));
    }

    public List<Pair<String, List<String>>> readGroups(final Set<String> recordIds) {
        final Key[] keys = recordIds.stream().map(id -> new Key(namespace, tempSet, id)).toArray(Key[]::new);

        final Record[] records = db.dynamicBatchRead(keys, null, null);

        final List<Pair<String, List<String>>> result = new ArrayList<>(recordIds.size());
        for (int i = 0; i < recordIds.size(); i++) {
            // record should always exist
            if (records[i].getBoolean(MERGED_BIN)) {
                result.add(Pair.with(records[i].getString(GROUP_ID_BIN), null));
            } else {
                result.add(Pair.with(records[i].getString(GROUP_ID_BIN), (List<String>) records[i].getList(OTHER_RECORD_IDS_BIN)));
            }
        }

        return result;
    }


    // save pageRank results as properties
    public void setProperty(final Map<FireflyId, Object> values, final String propertyName) {
        if (values.isEmpty()) {
            return;
        }

        final Long schemaVpKey = this.db.schemaManager.getVertexPropertyWrite(propertyName);

        final MapPolicy treeMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final MapPolicy hashMapPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);
        final BatchWritePolicy writePolicy = new BatchWritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        final List<BatchRecord> batchRecords = new ArrayList<>();
        for (final Map.Entry<FireflyId, Object> entry : values.entrySet()) {
            final Key recordKey = FireflyRecord.getKey(this.db, this.db.getConfig().vertexAeroSet, entry.getKey());
            if (isGeoVertexPropertyValue(this.db, propertyName, entry.getValue())) {
                final Long geoSchemaKey = this.db.schemaManager.getGeoPropertyWrite(propertyName);
                @SuppressWarnings("unchecked")
                final List<String> points = (List<String>) validateAndConvertVertexPropertyValue(
                        entry.getValue(), propertyName, this.db.getConfig());
                final Operation writeGeoData = MapOperation.put(hashMapPolicy, this.db.getConfig().geoDataBin,
                        Value.get(geoSchemaKey), Value.get(FireflyGeoValue.toGeoJsonValues(points)));
                batchRecords.add(new BatchWrite(writePolicy, recordKey, new Operation[]{writeGeoData}));
                this.graph.createGeoPropertyIndexIfNeeded(propertyName);
                continue;
            }
            final FireflyId vertexPropertyId = this.db.getIdFactory().generateId(this.graph, FireflyVertexProperty.class);
            final Long vpIdKey = (Long) vertexPropertyId.getStorageId();
            final Object typeHint = getTypeHintOf(entry.getValue(), true);
            final Object verifiedValue = validateAndConvertVertexPropertyValue(entry.getValue());

            final List<Long> idInList = new ArrayList<>(1);
            idInList.add(vpIdKey);
            final Map<Object, List<Long>> valueToIdList = new HashMap<>();
            valueToIdList.put(verifiedValue, idInList);
            final Operation writeVpData = MapOperation.put(treeMapPolicy, this.db.getConfig().vertexPropertyDataBin, Value.get(schemaVpKey), Value.get(valueToIdList));

            final Map<Long, Object> idToTypeHint = new HashMap<>();
            idToTypeHint.put(vpIdKey, typeHint);
            final Operation writeVpTypeHint = MapOperation.put(hashMapPolicy, this.db.getConfig().vertexPropertyTHBin, Value.get(schemaVpKey), Value.get(idToTypeHint));

            final Map<Long, Map<Long, List<Object>>> idToProperties = new HashMap<>();
            idToProperties.put(vpIdKey, Collections.emptyMap());
            final Operation writeVpProperties = MapOperation.put(hashMapPolicy, this.db.getConfig().vpPropertyBin, Value.get(schemaVpKey), Value.get(idToProperties));

            batchRecords.add(new BatchWrite(writePolicy, recordKey, new Operation[]{writeVpData, writeVpTypeHint, writeVpProperties}));
        }

        writeWithBackoff(batchRecords);
    }

    @FunctionalInterface
    interface AerospikeOperation {
        Object execute();
    }

    private Object tryWithBackoff(AerospikeOperation operation) {
        int backoff = 1;
        int backoffCount = 10;
        while (backoffCount-- > 0) {
            try {
                return operation.execute();
            } catch (final AerospikeException | AerospikeGraphException e) {
                final int errorCode = e instanceof AerospikeException
                        ? ((AerospikeException) e).getResultCode()
                        : ((AerospikeGraphException) e).errorCode;
                if (RETRYABLE_CODES.contains(errorCode)) {
                    try {
                        Thread.sleep(backoff * 1000L);
                    } catch (final InterruptedException ignored) {
                    }

                    // 1, 2, 4, 8, 10, 10, 10, ...
                    // 75 seconds max wait
                    if (backoff < 5) {
                        backoff *= 2;
                    } else {
                        backoff = 10;
                    }
                } else {
                    throw new RuntimeException("Failed to write after exponentially backing off 10 times.", e);
                }
            }
        }
        return null;
    }

    private void writeWithPaging(final List<BatchRecord> batchRecords) {
        for (int i = 0; i < batchRecords.size(); i += WRITE_PAGE_SIZE) {
            final int end = Math.min(batchRecords.size(), i + WRITE_PAGE_SIZE);
            writeWithBackoff(batchRecords.subList(i, end));
        }
    }

    private void writeWithBackoff(final List<BatchRecord> batchRecords) {
        tryWithBackoff(() -> {
            final BatchPolicy policy = new BatchPolicy();
            policy.setMaxConcurrentThreads(2);
            db.batchOperate(policy, batchRecords);
            return null;
        });
    }

    // jobs methods
    public void writeJob(final Job job) {
        final Key key = new Key(namespace, jobSet, job.getId());
        // todo: GRAPH-1562, add TTL, probably configurable
        db.checkedPut(null, key, job.asBins());
    }

    public Job readJob(final String jobId) {
        final Key key = new Key(namespace, jobSet, jobId);
        final Record record = db.read(key, null);
        if (record == null) {
            return null;
        }

        return new Job(record);
    }

    public void finishJob(final String jobId, final int resultsCount) {
        final Job job = readJob(jobId);
        if (job != null) {
            job.finish(resultsCount);
            writeJob(job);
        }
    }

    public void cancelAllJobs() {
        final Statement statement = new Statement();
        statement.setNamespace(db.getNamespace());
        statement.setSetName(jobSet);
        statement.setFilter(Filter.equal("state", "STARTED"));

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.setExpectedDuration(QueryDuration.SHORT);

        // active jobs count should be 0-1, so it's ok to write it one by one
        final FireflyRecordSet recordSet = db.query(queryPolicy, statement);
        for (final KeyRecord keyRecord : recordSet) {
            final Job job = new Job(keyRecord.record);
            job.cancel();
            writeJob(job);
        }
    }

    public void setJobError(final String jobId, final String error) {
        final Job job = readJob(jobId);
        // job will get error after cancellation
        if (job != null && job.getState() != Job.State.CANCELLED) {
            job.error(error);
            writeJob(job);
        }
    }

    public Job setJobIteration(final String jobId, final int iteration) {
        final Job job = readJob(jobId);
        if (job != null) {
            job.setIteration(iteration);
            writeJob(job);
        }
        return job;
    }

    public List<Job> getJobs() {
        final Queue<Record> records = new ConcurrentLinkedQueue<>();
        db.scanAll(null, jobSet, (key, record) -> records.add(record));
        return records.stream().map(Job::new).collect(Collectors.toList());
    }

    public Job getFirstRunningJob() {
        final Statement statement = new Statement();
        statement.setNamespace(db.getNamespace());
        statement.setSetName(jobSet);
        statement.setFilter(Filter.equal("state", "STARTED"));

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.setExpectedDuration(QueryDuration.SHORT);
        // active jobs count should be 0-1
        final FireflyRecordSet recordSet = db.query(queryPolicy, statement);
        for (final KeyRecord keyRecord : recordSet) {
            return new Job(keyRecord.record);
        }
        return null;
    }

    // For testing only.
    public void removeAllJobs() {
        final InfoPolicy infoPolicy = new InfoPolicy();
        db.setInfoPolicy(infoPolicy);
        db.truncate(infoPolicy, jobSet, null);
    }

    public Record getPackedRecord(final String vertexId, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        return db.read(key, null);
    }
}
