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

package com.aerospike.firefly.runtime.tasks;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

public class FireflyConfigurationTask extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyConfigurationTask.class);
    private final AerospikeConnection connection;

    public FireflyConfigurationTask(final AerospikeConnection connection) {
        this.connection = connection;
    }

    // Periodic execution.
    public void run() {
        try {
            connection.refreshConfiguration();
        } catch (Exception ex) {
            LOG.error("Error in FireflyConfiguration update thread:", ex);
        }
    }
}
