package com.aerospike.firefly.util;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.UUID;

public class StorageEfficiencyUtil {

    public static class StorageEfficiencyReport {
        private static class ByteSizeConstants {
            public static final long RECORD_OVERHEAD = 35;
            public static final long SET_NAME_OVERHEAD = 1;
            public static final long INTEGER_OVERHEAD = 2;
            public static final long BOOLEAN_OVERHEAD = 1;
            public static final long DOUBLE_OVERHEAD = 1;
            public static final long OBJECT_OVERHEAD = 5;
            public static final long NONZERO_TTL_VALUE = 4;
            public static final long TOMBSTONE_VALUE = 1;
            public static final long INTEGER_VALUE_255 = 1;
            public static final long INTEGER_VALUE_65535 = 2;
            public static final long INTEGER_VALUE_16777215 = 3;
            public static final long INTEGER_VALUE_0x7fffffff = 4;

        }

        private long calculateSetNameOverhead(final String setName) {
            return ByteSizeConstants.SET_NAME_OVERHEAD + setName.getBytes().length;
        }

        private long calculateRecordKeyOverhead(final Key key) {
            long overhead = 0;
            final long keySize = key.userKey.estimateSize();
            overhead++;
            if (keySize < 16384)
                overhead++;
            if (keySize >= 16384)
                overhead++;
            overhead++;
            return overhead + keySize;
        }


        //         * + 1-3 bytes overhead (1 byte for key size <128 bytes, 2 bytes for <16K, 3 bytes for >= 16K) + 1 byte (key type overhead) + flat key size
//         *
//         * Bin count overhead. No overhead for single-bin and tombstone records:
//         *
//         * +1 byte for count < 128, +2 bytes for < 16K, or +3 bytes for >= 16K
//         *
//         * General overhead for each bin. No overhead for single-bin:
//         *
//         * + 1 byte + bin name length in bytes and
//         *
//         * + 6 bytes for LUT depending on XDR bin-policy and
//         *
//         * + 1 byte for src-id if XDR bin convergence is enabled.
//         */
        private long calculateRecordBinOverhead(final Record record) {

            long overhead = 0;
            // number of bins
            // +1 byte for count < 128, +2 bytes for < 16K, or +3 bytes for >= 16K
            overhead++;
            if (record.bins.size() > 128)
                overhead++;
            if (record.bins.size() > 16384)
                overhead++;
            overhead += record.bins.size();
            for (String key : record.bins.keySet()) {
                // bin name length in bytes
                overhead += key.getBytes().length;
            }
            //@todo account for XDR

            for (Object value : record.bins.values()) {

                // bin value length in bytes
                overhead += value.toString().getBytes().length;
            }

            return overhead;
        }

        private long calculateRecordBinValueOverhead(final Record record) {
            long overhead = 0;
            for (Object value : record.bins.values()) {
                if (value.getClass().equals(String.class)) {
                    overhead += ByteSizeConstants.OBJECT_OVERHEAD;

                }
                // bin value length in bytes
                overhead += value.toString().getBytes().length;
            }
            return 0;
        }

        private final Record record;

        private StorageEfficiencyReport(Record record) {
            this.record = record;
        }

        public StorageEfficiencyReport build(Key key, Record record) {
            final long setNameOverhead = calculateSetNameOverhead(key.setName);
            final long recordKeyOverhead = calculateRecordKeyOverhead(key);
            final long recordBinOverhead = calculateRecordBinOverhead(record);
            final long ttlOverhead = record.getTimeToLive() > 0 ? ByteSizeConstants.NONZERO_TTL_VALUE : 0;
            return new StorageEfficiencyReport(record);
        }

    }

}
