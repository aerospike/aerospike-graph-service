package com.aerospike.firefly.olap.helper;

import com.aerospike.firefly.structure.util.LogInfo;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;

public class TaskLogger implements LogInfo {
    private TaskLogger () {
    }

    public static TaskLogger instance = new TaskLogger();

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
        instance.debuggingMessage(message, logger);
    }

}
