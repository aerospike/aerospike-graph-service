package com.aerospike.firefly.io.utils;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.USER_SUPPLIED_ID_CACHE_SET;

/**
 * We need our expected insertion count to grow with the database, so to do that we can use multiple
 * bloom filters, this also has the advantage of keeping any single bloom filter small, so when we check an id
 * it will be quick to load in and out of memory. If we instead set our expected size to something like
 * 1 billion and our desired false positive probability to 1% we would wind up with a gig or so loading in and out
 * for each request.
 *
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
// Suppress unstable API. The bloom filter provided by Google has been marked with @Beta since it was release (>10 years ago).
@SuppressWarnings("UnstableApiUsage")
public final class BloomFilterIdCache {
    public static final long BLOOM_FILTER_EXPECTED_INSERTIONS = 1 << 15;
    public static final long BLOOM_FILTER_BIT_MASK = -(1 << 15);
    private static final Funnel<Long> FUNNEL = Funnels.longFunnel();
    private static final int RETRY_COUNT = 100;
    private static final Object LOCK = new Object();
    private static BloomFilter<Long> bloomFilter = null;
    private static String bloomFilterid = null;

    private BloomFilterIdCache() {
    }

    /**
     * Check if id is available using bloom filter. If it is, take it.
     * @param client Aerospike client.
     * @param namespace namespace of graph.
     * @param name name of id cache.
     * @param id id to use.
     * @return true if id is available and is now in use, false otherwise.
     * @throws IOException If unable to determine whether id is available.
     */
    public static boolean takeIdIfAvailable(final AerospikeClient client, final String namespace, final String name, long id) {
        // Generate key and ClientPolicy.
        final Key key = new Key(namespace, USER_SUPPLIED_ID_CACHE_SET, name);
        final ClientPolicy clientPolicy = new ClientPolicy();

        // To ensure concurrent accesses do not result in corruption, we need to use GenerationPolicy.EXPECT_GEN_EQUAL.
        clientPolicy.writePolicyDefault = new WritePolicy();
        clientPolicy.writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;

        for (int i = 0; i < RETRY_COUNT; i++) {
            try {
                synchronized (LOCK) {
                    if (i == 0 && bloomFilter != null && idToBinKey(id).equals(bloomFilterid)) {

                        // If bloom filter might contain id, return false.
                        if (bloomFilter.mightContain(id)) {
                            return false;
                        }

                        // Add id to bloom filter and write bloom filter back.
                        bloomFilter.put(id);
                        putBloomFilter(client, key, idToBinKey(id), bloomFilter, clientPolicy);

                        // Return true.
                        return true;
                    }

                    // Grab bloom filter from aerospike.
                    bloomFilter = getBloomFilter(client, key, idToBinKey(id), clientPolicy);

                    // If bloom filter might contain id, return false.
                    if (bloomFilter.mightContain(id)) {
                        return false;
                    }

                    // Add id to bloom filter and write bloom filter back.
                    bloomFilter.put(id);
                    putBloomFilter(client, key, idToBinKey(id), bloomFilter, clientPolicy);

                    // Return true.
                    return true;
                }
            } catch (AerospikeException ignored) {
                // Occurs when the read/modify/write notices another write has occurred before it finished.
                // Ignore exception and try again.
            } catch (IOException e) {
                // This should never happen (famous last words).
                // This would indicate corruption in aerospike.
                throw new IllegalStateException("Failed to determine if user id is in use.");
            }
        }
        return false;
    }

    private static String idToBinKey(long id) {
        return ((Long) (id & BLOOM_FILTER_BIT_MASK)).toString();
    }

    private static BloomFilter<Long> getBloomFilter(final AerospikeClient client, final Key key, final String binKey, ClientPolicy clientPolicy) throws IOException {
        // Check if it exists, if it does not create it.
        boolean exists = client.exists(clientPolicy.readPolicyDefault, key);
        if (!exists) {
            return BloomFilter.create(FUNNEL, BLOOM_FILTER_EXPECTED_INSERTIONS);
        }

        // Read byte array form of bloom filter from aerospike.
        final Record record = client.get(clientPolicy.readPolicyDefault, key);
        final byte[] data = (byte[]) record.bins.get(binKey);

        // Set WritePolicy generation based on Record.
        clientPolicy.writePolicyDefault.generation = record.generation;

        // Return bloom filter loaded from byte array.
        return BloomFilter.readFrom(new ByteArrayInputStream(data), FUNNEL);
    }

    private static void putBloomFilter(final AerospikeClient client, final Key key, final String binKey, final BloomFilter<Long> bloomFilter, ClientPolicy clientPolicy) throws IOException {
        // Convert bloom filter to byte array.
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        bloomFilter.writeTo(outputStream);

        // Write byte array into aerospike.
        byte[] data = outputStream.toByteArray();
        clientPolicy.writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        client.put(clientPolicy.writePolicyDefault, key, new Bin(binKey, data));
        clientPolicy.writePolicyDefault.generation++;
    }
}
