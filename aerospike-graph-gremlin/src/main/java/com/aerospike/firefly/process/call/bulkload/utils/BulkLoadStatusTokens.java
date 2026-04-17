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

package com.aerospike.firefly.process.call.bulkload.utils;

public class BulkLoadStatusTokens {
    public static String LOAD_STEP = "step";
    public static String PARTITIONS_COMPLETED_PERCENTAGE = "complete-partitions-percentage";
    public static String ELEMENTS_WRITTEN = "elements-written";
    public static String PROGRESS_COMPLETE = "complete";

    public static String BULK_LOAD_STATUS_KEY = "status";
    public static String BULK_LOAD_STATUS_IN_PROGRESS = "in progress";
    public static String BULK_LOAD_STATUS_SUCCESS = "success";
    public static String BULK_LOAD_STATUS_ERROR = "error";

    public static String BULK_LOAD_EXCEPTION_MESSAGE = "message";
    public static String BULK_LOAD_EXCEPTION_STACKTRACE = "stacktrace";

    private BulkLoadStatusTokens() {

    }
}
