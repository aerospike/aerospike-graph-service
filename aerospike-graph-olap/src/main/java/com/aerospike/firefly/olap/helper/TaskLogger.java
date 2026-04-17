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

package com.aerospike.firefly.olap.helper;

import com.aerospike.firefly.structure.util.LogInfo;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;

public class TaskLogger implements LogInfo {
    private TaskLogger () {
    }

    public static TaskLogger instance = new TaskLogger();
    boolean debugging = false;

    private static String getTaskInfo() {
        final TaskContext taskContext = TaskContext.get();
        if (taskContext == null) {
            return "Task Master [x x x]";
        } else {
            return String.format("Task %d - [%s %s %s]", taskContext.getPartitionId(),
                    taskContext.taskAttemptId(),
                    taskContext.attemptNumber(),
                    taskContext.stageId());
        }
    }

    public void debuggingMessage(final String message, final Logger logger) {
        logger.info(getTaskInfo() + " " + message);
    }

    public static void logDebuggingMessage(final String message, final Logger logger) {
        if (instance.debugging)
            instance.debuggingMessage(message, logger);
    }

    public void setDebugging(final boolean debugging) {
        this.debugging = debugging;
    }
}
