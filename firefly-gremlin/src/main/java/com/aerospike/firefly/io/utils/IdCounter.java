package com.aerospike.firefly.io.utils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.ArrayList;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.COUNTER;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ID_MANAGER_SET;

public final class IdCounter {
    private IdCounter() {
    }

    /**
     * get the current value of an Id counter
     *
     * @param name name of Counter
     * @return value of counter
     */
    public static long getIdCounter(final String name, final String namespace, AerospikeClient client) {
        final Record record = client.get(null, new Key(namespace, ID_MANAGER_SET, name));
        return record.getLong(COUNTER);
    }

    /**
     * Increment an Id counter by a supplied value and return its incremented value
     *
     * @param name      name of Counter to operate on
     * @param increment value to increment by
     * @return value of counter after operation
     */
    public static long incrementAndGetIdCounter(final String name, long increment, final String namespace, AerospikeClient client) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, increment);
        final Record record = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return record.getLong(COUNTER);
    }

    /**
     * Increment an Id counter by 1 and return its incremented value
     *
     * @param name name of Counter to operate on
     * @return value of Counter after operation
     */
    public static long incrementAndGetIdCounter(final String name, final String namespace, AerospikeClient client) {

        return incrementAndGetIdCounter(name, 1, namespace, client);
    }

    /**
     * decrement an Id counter
     *
     * @param name name of Counter to operate on
     * @return value of counter after operation
     */
    public static long decrementIdCounter(final String name, final String namespace, AerospikeClient client) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        final Bin ctr = new Bin(COUNTER, -1);
        final Record record = client.operate(null, key,
                Operation.add(ctr),
                Operation.get(COUNTER));
        return record.getLong(COUNTER);
    }

    /**
     * Zero an Id counter
     *
     * @param name name of Counter to operate on
     * @return value of counter after operation
     */
    public static long zeroIdCounter(final String name, AerospikeConnection connection) {
        final Bin ctr = new Bin(COUNTER, 0);
        FireflyRecord.write(connection, ID_MANAGER_SET, FireflyId.of(connection, null, name), ctr);
        return 0L;
    }

    /**
     * Offer a value, compare it to the current counter value.
     * if the offered value is greater then the current counter value
     * set the counter to the offered value, and return it.
     * otherwise, increment the counter by 1, and return that.
     *
     * @param offer proposed value
     * @param name  name of counter
     * @return Incremented counter value or offered value
     */

    public static long greaterOrIncrement(final long offer, final String name, final String namespace, final AerospikeClient client) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        Expression gtexp = Exp.build(Exp.cond(
                Exp.gt(
                        Exp.val(offer),
                        Exp.add(Exp.intBin(COUNTER), Exp.val(1))
                ),
                Exp.val(offer),
                Exp.add(Exp.intBin(COUNTER), Exp.val(1))
        ));
        Record result = client.operate(null, key, ExpOperation.write(COUNTER, gtexp, ExpWriteFlags.DEFAULT), Operation.get(COUNTER));
        ArrayList<Object> ret = (ArrayList<Object>) result.getValue(COUNTER);
        return (long) ret.get(1);
    }

    /**
     * over a value and name a counter. return the greater of the two.
     *
     * @param offer proposed value
     * @param name  name of counter to operate on
     * @return value of counter or proposed value
     */
    public static long greaterOrExisting(final long offer, final String name, final String namespace, final AerospikeClient client) {
        final Key key = new Key(namespace, ID_MANAGER_SET, name);
        Expression gtexp = Exp.build(Exp.cond(
                Exp.gt(
                        Exp.val(offer),
                        Exp.add(Exp.intBin(COUNTER), Exp.val(1))
                ),
                Exp.val(offer),
                Exp.intBin(COUNTER)
        ));
        Record result = client.operate(null, key, ExpOperation.write(COUNTER, gtexp, ExpWriteFlags.DEFAULT), Operation.get(COUNTER));
        ArrayList<Object> ret = (ArrayList<Object>) result.getValue(COUNTER);
        return (long) ret.get(1);
    }
}
