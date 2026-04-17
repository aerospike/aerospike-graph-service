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

package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AerospikeLogger implements Log.Callback {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeClient.class);

    @Override
    public void log(final Log.Level level, final String s) {
        if (level == Log.Level.INFO) {
            LOG.info(s);
        } else if (level == Log.Level.WARN) {
            LOG.warn(s);
        } else if (level == Log.Level.ERROR) {
            LOG.error(s);
        } else if (level == Log.Level.DEBUG) {
            LOG.debug(s);
        } else {
            LOG.info(s);
        }
    }
}
