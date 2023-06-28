package com.aerospike.firefly.bulkloader.integration.util;

import java.lang.reflect.InvocationTargetException;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.AerospikeException.InvalidNode;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.IndexListener;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchUDFPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.Util;

public class FailingAerospikeClient implements IAerospikeClient {
    private final FailureProfile failureProfile;
    private final IAerospikeClient delegate;

    private interface Invoker<T extends Policy> {
        Object invoke(T policy);
    }

    private static class TimeoutHandler<T extends Policy> {
        private final T policy;
        private long deadline;
        private int iteration = 1;
        private int maxRetries;
        private int totalTimeout;
        private int socketTimeout;

        @SuppressWarnings("unchecked")
        private T clonePolicy(T original, Class<? extends Policy> type) {
            if (original == null) {
                try {
                    return (T) type.getDeclaredConstructor().newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                } catch (SecurityException e) {
                    throw new RuntimeException(e);
                }
            } else if (original instanceof WritePolicy) {
                return (T) new WritePolicy(original);
            } else if (original instanceof ScanPolicy) {
                return (T) new ScanPolicy(original);
            } else if (original instanceof QueryPolicy) {
                return (T) new QueryPolicy(original);
            } else if (original instanceof BatchPolicy) {
                return (T) new BatchPolicy(original);
            } else {
                return (T) new Policy(original);
            }
        }

        public TimeoutHandler(T policy, Class<? extends Policy> requiredClass) {
            this.policy = clonePolicy(policy, requiredClass);
            this.maxRetries = this.policy.maxRetries;
            this.totalTimeout = this.policy.totalTimeout;
            this.socketTimeout = this.policy.socketTimeout;
            if (this.policy.totalTimeout > 0) {
                this.deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.policy.totalTimeout);
            }
            // Override the timeout parameters on the socket as we're taking care of them ourselves. This allows the
            // interceptors to behave like the real client would even in the face of a faked timeout
            this.policy.maxRetries = 0;
        }

        public <R> R invoke(Invoker<T> invoker) {
            Log.debug("Pre call");
            AerospikeException exception = null;
            R result = null;
            while (true) {
                try {
                    this.policy.totalTimeout = this.totalTimeout;
                    this.policy.socketTimeout = this.socketTimeout;
                    result = (R) invoker.invoke(this.policy);
                    Log.debug("post call - successful");
                    return result;
                } catch (AerospikeException ae) {
                    exception = ae;
                    if (ae.getResultCode() != ResultCode.TIMEOUT && ae.getResultCode() != ResultCode.DEVICE_OVERLOAD) {
                        Log.debug("post call - failed with " + ae.getClass().getCanonicalName());
                        throw ae;
                    }
                }
                if (iteration > maxRetries) {
                    break;
                }
                if (totalTimeout > 0) {
                    long remainingNs = deadline - System.nanoTime()
                            - TimeUnit.MILLISECONDS.toNanos(policy.sleepBetweenRetries);
                    if (remainingNs <= 0) {
                        break;
                    }
                    long remainingMs = TimeUnit.NANOSECONDS.toMillis(remainingNs);
                    if (remainingMs < totalTimeout) {
                        totalTimeout = (int) remainingMs;
                        if (socketTimeout > totalTimeout) {
                            socketTimeout = totalTimeout;
                        }
                    }
                }
                if (policy.sleepBetweenRetries > 0) {
                    Util.sleep(policy.sleepBetweenRetries);
                }
                iteration++;
            }
            Log.debug("post call - failed after exhausting retries with " + exception.getClass().getCanonicalName());
            throw exception;
        }
    }

    static public IAerospikeClient clientWithWriteFails(final IAerospikeClient aerospikeClient, final double failChance) {
        final FailureProfile profile = new FailureProfile().chanceOfWriteTimeout(failChance);
        profile.enable();
        return new FailingAerospikeClient(aerospikeClient, profile);
    }

    public FailingAerospikeClient(final IAerospikeClient aerospikeClient, FailureProfile failureProfile)
            throws AerospikeException {
        this.delegate = aerospikeClient;
        this.failureProfile = failureProfile;
    }

    @Override
    public final Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            return delegate.operate(policy, key, operations);
        }
        Record r = new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
            failureProfile.preWriteTxn();
            Record result = delegate.operate(newPolicy, key, operations);
            failureProfile.postWriteTxn();
            return result;
        });
        return r;
    }

    @Override
    public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            return delegate.getHeader(policy, keys);
        }
        Record[] r = new TimeoutHandler<BatchPolicy>(policy, BatchPolicy.class).invoke((newPolicy) -> {
            failureProfile.preReadTxn();
            Record[] results = delegate.getHeader(newPolicy, keys);
            failureProfile.postReadTxn();
            return results;
        });
        return r;
    }

    public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            delegate.put(policy, key, bins);
            return;
        }
        new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
            failureProfile.preWriteTxn();
            delegate.put(newPolicy, key, bins);
            failureProfile.postWriteTxn();
            return null;
        });
    }

    public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            delegate.append(policy, key, bins);
            return;
        }
        new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
            failureProfile.preWriteTxn();
            delegate.append(newPolicy, key, bins);
            failureProfile.postWriteTxn();
            return null;
        });
    }

    public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            delegate.prepend(policy, key, bins);
            return;
        }
        new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
            failureProfile.preWriteTxn();
            delegate.prepend(newPolicy, key, bins);
            failureProfile.postWriteTxn();
            return null;
        });
    }

    public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            delegate.add(policy, key, bins);
            return;
        }
        new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
            failureProfile.preWriteTxn();
            delegate.add(newPolicy, key, bins);
            failureProfile.postWriteTxn();
            return null;
        });
    }

    public boolean delete(WritePolicy policy, Key key) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            return delegate.delete(policy, key);
        }
        boolean r = new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
            failureProfile.preWriteTxn();
            boolean result = delegate.delete(newPolicy, key);
            failureProfile.postWriteTxn();
            return result;
        });
        return r;
    }

    public void touch(WritePolicy policy, Key key) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            delegate.touch(policy, key);
            return;
        }
        new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
            failureProfile.preWriteTxn();
            delegate.touch(newPolicy, key);
            failureProfile.postWriteTxn();
            return null;
        });
    }

    public boolean exists(Policy policy, Key key) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            return delegate.exists(policy, key);
        }
        boolean r = new TimeoutHandler<Policy>(policy, Policy.class).invoke((newPolicy) -> {
            failureProfile.preReadTxn();
            boolean result = delegate.exists(newPolicy, key);
            failureProfile.postReadTxn();
            return result;
        });
        return r;
    }

    public Record get(Policy policy, Key key) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            return delegate.get(policy, key);
        }
        Record r = new TimeoutHandler<Policy>(policy, Policy.class).invoke((newPolicy) -> {
            failureProfile.preReadTxn();
            Record result = delegate.get(newPolicy, key);
            failureProfile.postReadTxn();
            return result;
        });
        return r;
    }

    public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            return delegate.get(policy, key, binNames);
        }
        Record r = new TimeoutHandler<Policy>(policy, Policy.class).invoke((newPolicy) -> {
            failureProfile.preReadTxn();
            Record result = delegate.get(newPolicy, key, binNames);
            failureProfile.postReadTxn();
            return result;
        });
        return r;
    }

    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        if (!failureProfile.isEnabled()) {
            return delegate.getHeader(policy, key);
        }
        Record r = new TimeoutHandler<Policy>(policy, Policy.class).invoke((newPolicy) -> {
            failureProfile.preReadTxn();
            Record result = delegate.getHeader(newPolicy, key);
            failureProfile.postReadTxn();
            return result;
        });
        return r;
    }

    // -------------------------------------------------------------------------

    public void close() {
        delegate.close();
    }

    public boolean isConnected() {
        return delegate.isConnected();
    }

    public Node[] getNodes() {
        return delegate.getNodes();
    }

    public List<String> getNodeNames() {
        return delegate.getNodeNames();
    }

    public Node getNode(String nodeName) throws InvalidNode {
        return delegate.getNode(nodeName);
    }

    public ClusterStats getClusterStats() {
        return delegate.getClusterStats();
    }

    public Cluster getCluster() {
        return delegate.getCluster();
    }

    public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {
        delegate.put(eventLoop, listener, policy, key, bins);
    }

    public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {
        delegate.append(eventLoop, listener, policy, key, bins);
    }

    public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {
        delegate.prepend(eventLoop, listener, policy, key, bins);
    }

    public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {
        delegate.add(eventLoop, listener, policy, key, bins);
    }

    public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key)
            throws AerospikeException {
        delegate.delete(eventLoop, listener, policy, key);
    }

    public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate)
            throws AerospikeException {
        delegate.truncate(policy, ns, set, beforeLastUpdate);
    }

    public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key)
            throws AerospikeException {
        delegate.touch(eventLoop, listener, policy, key);
    }

    public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) throws AerospikeException {
        delegate.exists(eventLoop, listener, policy, key);
    }

    public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return delegate.exists(policy, keys);
    }

    public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        delegate.exists(eventLoop, listener, policy, keys);
    }

    public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        delegate.exists(eventLoop, listener, policy, keys);
    }

    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
        delegate.get(eventLoop, listener, policy, key);
    }

    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames)
            throws AerospikeException {
        delegate.get(eventLoop, listener, policy, key, binNames);
    }

    public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)
            throws AerospikeException {
        delegate.getHeader(eventLoop, listener, policy, key);
    }

    public boolean get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        return delegate.get(policy, records);
    }

    public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records)
            throws AerospikeException {
        delegate.get(eventLoop, listener, policy, records);
    }

    public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records)
            throws AerospikeException {
        delegate.get(eventLoop, listener, policy, records);
    }

    public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return delegate.get(policy, keys);
    }

    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        delegate.get(eventLoop, listener, policy, keys);
    }

    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        delegate.get(eventLoop, listener, policy, keys);
    }

    public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        return delegate.get(policy, keys, binNames);
    }

    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys,
            String... binNames) throws AerospikeException {
        delegate.get(eventLoop, listener, policy, keys, binNames);
    }

    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys,
            String... binNames) throws AerospikeException {
        delegate.get(eventLoop, listener, policy, keys, binNames);
    }

    public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        delegate.getHeader(eventLoop, listener, policy, keys);
    }

    public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        delegate.getHeader(eventLoop, listener, policy, keys);
    }

    public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key,
            Operation... operations) throws AerospikeException {
        delegate.operate(eventLoop, listener, policy, key, operations);
    }

    public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames)
            throws AerospikeException {
        delegate.scanAll(policy, namespace, setName, callback, binNames);
    }

    public void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace,
            String setName, String... binNames) throws AerospikeException {
        delegate.scanAll(eventLoop, listener, policy, namespace, setName, binNames);
    }

    public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback,
            String... binNames) throws AerospikeException {
        delegate.scanNode(policy, nodeName, namespace, setName, callback, binNames);
    }

    public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback,
            String... binNames) throws AerospikeException {
        delegate.scanNode(policy, node, namespace, setName, callback, binNames);
    }

    public void scanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName,
            ScanCallback callback, String... binNames) throws AerospikeException {
        delegate.scanPartitions(policy, partitionFilter, namespace, setName, callback, binNames);
    }

    public void scanPartitions(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy,
            PartitionFilter partitionFilter, String namespace, String setName, String... binNames)
            throws AerospikeException {
        delegate.scanPartitions(eventLoop, listener, policy, partitionFilter, namespace, setName, binNames);
    }

    public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language)
            throws AerospikeException {
        return delegate.register(policy, clientPath, serverPath, language);
    }

    public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath,
            Language language) throws AerospikeException {
        return delegate.register(policy, resourceLoader, resourcePath, serverPath, language);
    }

    public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language)
            throws AerospikeException {
        return delegate.registerUdfString(policy, code, serverPath, language);
    }

    public void removeUdf(InfoPolicy policy, String serverPath) throws AerospikeException {
        delegate.removeUdf(policy, serverPath);
    }

    public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args)
            throws AerospikeException {
        return delegate.execute(policy, key, packageName, functionName, args);
    }

    public void execute(EventLoop eventLoop, ExecuteListener listener, WritePolicy policy, Key key, String packageName,
            String functionName, Value... functionArgs) throws AerospikeException {
        delegate.execute(eventLoop, listener, policy, key, packageName, functionName, functionArgs);
    }

    public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName,
            Value... functionArgs) throws AerospikeException {
        return delegate.execute(policy, statement, packageName, functionName, functionArgs);
    }

    public ExecuteTask execute(WritePolicy policy, Statement statement, Operation... operations)
            throws AerospikeException {
        return delegate.execute(policy, statement, operations);
    }

    public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
        return delegate.query(policy, statement);
    }

    public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement)
            throws AerospikeException {
        delegate.query(eventLoop, listener, policy, statement);
    }

    public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        return delegate.queryNode(policy, statement, node);
    }

    public RecordSet queryPartitions(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter)
            throws AerospikeException {
        return delegate.queryPartitions(policy, statement, partitionFilter);
    }

    public void queryPartitions(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy,
            Statement statement, PartitionFilter partitionFilter) throws AerospikeException {
        delegate.queryPartitions(eventLoop, listener, policy, statement, partitionFilter);
    }

    public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName,
            Value... functionArgs) throws AerospikeException {
        return delegate.queryAggregate(policy, statement, packageName, functionName, functionArgs);
    }

    public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
        return delegate.queryAggregate(policy, statement);
    }

    public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        return delegate.queryAggregateNode(policy, statement, node);
    }

    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName,
            IndexType indexType) throws AerospikeException {
        return delegate.createIndex(policy, namespace, setName, indexName, binName, indexType);
    }

    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName,
            IndexType indexType, IndexCollectionType indexCollectionType) throws AerospikeException {
        return delegate.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType);
    }

    public void createIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace,
            String setName, String indexName, String binName, IndexType indexType,
            IndexCollectionType indexCollectionType) throws AerospikeException {
        delegate.createIndex(eventLoop, listener, policy, namespace, setName, indexName, binName, indexType,
                indexCollectionType);
    }

    public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName)
            throws AerospikeException {
        return delegate.dropIndex(policy, namespace, setName, indexName);
    }

    public void dropIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace, String setName,
            String indexName) throws AerospikeException {
        delegate.dropIndex(eventLoop, listener, policy, namespace, setName, indexName);
    }

    public void info(EventLoop eventLoop, InfoListener listener, InfoPolicy policy, Node node, String... commands)
            throws AerospikeException {
        delegate.info(eventLoop, listener, policy, node, commands);
    }

    public void createUser(AdminPolicy policy, String user, String password, List<String> roles)
            throws AerospikeException {
        delegate.createUser(policy, user, password, roles);
    }

    public void dropUser(AdminPolicy policy, String user) throws AerospikeException {
        delegate.dropUser(policy, user);
    }

    public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {
        delegate.changePassword(policy, user, password);
    }

    public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        delegate.grantRoles(policy, user, roles);
    }

    public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        delegate.revokeRoles(policy, user, roles);
    }

    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        delegate.createRole(policy, roleName, privileges);
    }

    public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {
        delegate.dropRole(policy, roleName);
    }

    public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)
            throws AerospikeException {
        delegate.grantPrivileges(policy, roleName, privileges);
    }

    public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)
            throws AerospikeException {
        delegate.revokePrivileges(policy, roleName, privileges);
    }

    public User queryUser(AdminPolicy policy, String user) throws AerospikeException {
        return delegate.queryUser(policy, user);
    }

    public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
        return delegate.queryUsers(policy);
    }

    public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
        return delegate.queryRole(policy, roleName);
    }

    public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
        return delegate.queryRoles(policy);
    }

    @Override
    public Policy getReadPolicyDefault() {
        return delegate.getReadPolicyDefault();
    }

    @Override
    public WritePolicy getWritePolicyDefault() {
        return delegate.getWritePolicyDefault();
    }

    @Override
    public ScanPolicy getScanPolicyDefault() {
        return delegate.getScanPolicyDefault();
    }

    @Override
    public QueryPolicy getQueryPolicyDefault() {
        return delegate.getQueryPolicyDefault();
    }

    @Override
    public BatchPolicy getBatchPolicyDefault() {
        return delegate.getBatchPolicyDefault();
    }

    @Override
    public InfoPolicy getInfoPolicyDefault() {
        return delegate.getInfoPolicyDefault();
    }

    @Override
    public BatchPolicy getBatchParentPolicyWriteDefault() {
        return delegate.getBatchParentPolicyWriteDefault();
    }

    @Override
    public BatchWritePolicy getBatchWritePolicyDefault() {
        return delegate.getBatchWritePolicyDefault();
    }

    @Override
    public BatchDeletePolicy getBatchDeletePolicyDefault() {
        return delegate.getBatchDeletePolicyDefault();
    }

    @Override
    public BatchUDFPolicy getBatchUDFPolicyDefault() {
        return delegate.getBatchUDFPolicyDefault();
    }

    @Override
    public BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)
            throws AerospikeException {

        return delegate.delete(batchPolicy, deletePolicy, keys);
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy,
            BatchDeletePolicy deletePolicy, Key[] keys) throws AerospikeException {

        delegate.delete(eventLoop, listener, batchPolicy, deletePolicy, keys);
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy,
            BatchDeletePolicy deletePolicy, Key[] keys) throws AerospikeException {

        delegate.delete(eventLoop, listener, batchPolicy, deletePolicy, keys);
    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys, Operation... ops) throws AerospikeException {
        return delegate.get(policy, keys, ops);
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, Operation... ops)
            throws AerospikeException {
        delegate.get(eventLoop, listener, policy, keys);
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys,
            Operation... ops) throws AerospikeException {

        delegate.get(eventLoop, listener, policy, keys, ops);
    }

    @Override
    public boolean operate(BatchPolicy policy, List<BatchRecord> records) throws AerospikeException {
        return delegate.operate(policy, records);
    }

    @Override
    public void operate(EventLoop eventLoop, BatchOperateListListener listener, BatchPolicy policy,
            List<BatchRecord> records) throws AerospikeException {
        delegate.operate(eventLoop, listener, policy, records);
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy policy,
            List<BatchRecord> records) throws AerospikeException {
        delegate.operate(eventLoop, listener, policy, records);
    }

    @Override
    public BatchResults operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys, Operation... ops)
            throws AerospikeException {
        return delegate.operate(batchPolicy, writePolicy, keys, ops);
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy,
            BatchWritePolicy writePolicy, Key[] keys, Operation... ops) throws AerospikeException {
        delegate.operate(eventLoop, listener, batchPolicy, writePolicy, keys, ops);
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy,
            BatchWritePolicy writePolicy, Key[] keys, Operation... ops) throws AerospikeException {
        delegate.operate(eventLoop, listener, batchPolicy, null);
    }

    @Override
    public BatchResults execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys, String packageName,
            String functionName, Value... functionArgs) throws AerospikeException {

        return delegate.execute(batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy,
            BatchUDFPolicy udfPolicy, Key[] keys, String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        delegate.execute(eventLoop, listener, batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy,
            BatchUDFPolicy udfPolicy, Key[] keys, String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        delegate.execute(eventLoop, listener, batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
    }

    @Override
    public void query(QueryPolicy policy, Statement statement, QueryListener listener) throws AerospikeException {
        delegate.query(policy, statement, listener);
    }

    @Override
    public void query(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter, QueryListener listener)
            throws AerospikeException {
        delegate.query(policy, statement, partitionFilter, listener);
    }

    @Override
    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName,
            IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx) throws AerospikeException {
        return delegate.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType,
                ctx);
    }

    @Override
    public void createIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace,
            String setName, String indexName, String binName, IndexType indexType,
            IndexCollectionType indexCollectionType, CTX... ctx) throws AerospikeException {
        delegate.createIndex(eventLoop, listener, policy, namespace, setName, indexName, binName, indexType,
                indexCollectionType, ctx);
    }

    @Override
    public void setXDRFilter(InfoPolicy policy, String datacenter, String namespace, Expression filter)
            throws AerospikeException {
        delegate.setXDRFilter(policy, datacenter, namespace, filter);
    }

    @Override
    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist)
            throws AerospikeException {
        delegate.createRole(policy, roleName, privileges, whitelist);
    }

    @Override
    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist,
            int readQuota, int writeQuota) throws AerospikeException {
        delegate.createRole(policy, roleName, privileges, whitelist, readQuota, writeQuota);
    }

    @Override
    public void setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist) throws AerospikeException {
        delegate.setWhitelist(policy, roleName, whitelist);
    }

    @Override
    public void setQuotas(AdminPolicy policy, String roleName, int readQuota, int writeQuota)
            throws AerospikeException {
        delegate.setQuotas(policy, roleName, readQuota, writeQuota);
    }
}
