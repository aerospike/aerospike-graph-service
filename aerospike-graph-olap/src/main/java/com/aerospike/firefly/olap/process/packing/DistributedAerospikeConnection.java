package com.aerospike.firefly.olap.process.packing;


import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
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
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.QueryDuration;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.AerospikeConnection.FireflyRecordSet;
import com.aerospike.firefly.olap.structure.job.Job;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.util.FireflyHelper.validateAndConvertVertexPropertyValue;

public class DistributedAerospikeConnection {
    private final FireflyGraph graph;
    private final AerospikeConnection db;
    private final String namespace;
    // used for temporary run-time data for olap
    private final String tempSet;
    private final String jobSet = "jobs";
    private final long packedElementCount;
    private final long packSize;
    private final HashFunction hashingFunction = Hashing.murmur3_128();

    private final String bin = "b";
    private final String secondBin = "b2";

    public DistributedAerospikeConnection(final FireflyGraph graph,
                                          final String namespace,
                                          final String set,
                                          final long packedElementCount,
                                          final long packSize) {
        this.graph = graph;
        this.db = graph.getBaseGraph();
        this.namespace = namespace;
        this.tempSet = set;
        this.packedElementCount = packedElementCount;
        this.packSize = packSize;

        createJobIndex();
    }

    public DistributedAerospikeConnection(final FireflyGraph graph,
                                          final long packedElementCount,
                                          final long packSize) {
        this(graph, graph.getBaseGraph().getNamespace(), graph.getBaseGraph().OLAP_SET, packedElementCount, packSize);
    }

    public DistributedAerospikeConnection(final FireflyGraph graph) {
        this(graph, graph.fireflySummaryUpdater.getFireflyStatistics().totalVertexCount(), 10);
    }

    private void createJobIndex() {
        final List<String> existingIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());

        db.createIndex(existingIndexes, jobSet, "job_state", "state", IndexType.STRING, IndexCollectionType.DEFAULT);
    }

    // Truncate OLAP set.
    public void truncateOlapSet() {
        db.truncate(null, tempSet, null);
    }

    // Long accumulator functions.
    public void setAccumulator(final String name, final Long amount) {
        final Key key = new Key(namespace, tempSet, name);
        final Bin ctr = new Bin(bin, amount);
        db.writeOperate(null, key, Operation.put(ctr));
    }

    public Long getAccumulatorLong(final String name) {
        final Key key = new Key(namespace, tempSet, name);
        final Record record = db.writeOperate(null, key, Operation.get(bin));
        return record.getLong(bin);
    }

    public void addAccumulator(final String name, final Long amount) {
        final Key key = new Key(namespace, tempSet, name);
        final Bin ctr = new Bin(bin, amount);
        db.writeOperate(null, key, Operation.add(ctr));
    }

    // Double accumulator functions.

    public void setAccumulator(final String name, final Double amount) {
        final Key key = new Key(namespace, tempSet, name);
        final Bin ctr = new Bin(bin, amount);
        db.writeOperate(null, key, Operation.put(ctr));
    }

    public Double getAccumulatorDouble(final String name) {
        final Key key = new Key(namespace, tempSet, name);
        final Record record = db.writeOperate(null, key, Operation.get(bin));
        return record.getDouble(bin);
    }

    public void addAccumulator(final String name, final Double amount) {
        final Key key = new Key(namespace, tempSet, name);
        final Bin ctr = new Bin(bin, amount);
        db.writeOperate(null, key, Operation.add(ctr));
    }

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

        final BatchPolicy policy = new BatchPolicy();
        policy.setMaxConcurrentThreads(2);
        db.batchOperate(policy, batchRecords);
    }

    private static int allowedErrorPrints = 3;
    public Map<ByteArrayWrapper, Double> getPackedAccumulatorDoubleCache(final List<ByteArrayWrapper> vertexIds, final int iteration) {
        if (vertexIds.isEmpty()) {
            return Collections.emptyMap();
        }

        final String binName = bin + "_" + iteration;
        final int chunkSize = db.PAGINATION_PAGE_SIZE;
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

    // save pageRank results as properties
    public void setProperty(final Map<FireflyId, Double> values, final String propertyName) {
        if (values.isEmpty()) {
            return;
        }

        final Long schemaVpKey = this.db.schemaManager.getVertexPropertyWrite(propertyName);

        final MapPolicy treeMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final MapPolicy hashMapPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);
        final BatchWritePolicy writePolicy = new BatchWritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        final List<BatchRecord> batchRecords = new ArrayList<>();
        for (final Map.Entry<FireflyId, Double> entry : values.entrySet()) {
            final Key recordKey = getKey(this.db, this.db.VERTEX_AERO_SET, entry.getKey());
            final FireflyId vertexPropertyId = this.db.getIdFactory().generateId(this.graph, FireflyVertexProperty.class);
            final Long vpIdKey = (Long) vertexPropertyId.getStorageId();
            final Object typeHint = getTypeHintOf(entry.getValue(), true);
            final Object verifiedValue = validateAndConvertVertexPropertyValue(entry.getValue());

            final List<Long> idInList = new ArrayList<>(1);
            idInList.add(vpIdKey);
            final Map<Object, List<Long>> valueToIdList = new HashMap<>();
            valueToIdList.put(verifiedValue, idInList);
            final Operation writeVpData = MapOperation.put(treeMapPolicy, this.db.VERTEX_PROPERTY_DATA_BIN, Value.get(schemaVpKey), Value.get(valueToIdList));

            final Map<Long, Object> idToTypeHint = new HashMap<>();
            idToTypeHint.put(vpIdKey, typeHint);
            final Operation writeVpTypeHint = MapOperation.put(hashMapPolicy, this.db.VERTEX_PROPERTY_TH_BIN, Value.get(schemaVpKey), Value.get(idToTypeHint));

            final Map<Long, Map<Long, List<Object>>> idToProperties = new HashMap<>();
            idToProperties.put(vpIdKey, Collections.emptyMap());
            final Operation writeVpProperties = MapOperation.put(hashMapPolicy, this.db.VP_PROPERTY_BIN, Value.get(schemaVpKey), Value.get(idToProperties));

            batchRecords.add(new BatchWrite(writePolicy, recordKey, new Operation[]{writeVpData, writeVpTypeHint, writeVpProperties}));
        }

        final BatchPolicy policy = new BatchPolicy();
        policy.setMaxConcurrentThreads(2);
        db.batchOperate(policy, batchRecords);
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
        db.truncate(null, jobSet, null);
    }

    public Record getPackedRecord(final String vertexId, final int iteration) {
        final Key key = getPackedKeyFromId(vertexId);
        return db.read(key, null);
    }
}
