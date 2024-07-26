package com.aerospike.firefly.bulkloader.util;

import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProgressBar extends TimerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProgressBar.class);

    private final int intervalMillis;
    private FireflyGraph graph = null;
    private boolean isL2Mode = false;
    private boolean preflightCheckComplete = false;
    private boolean superNodeExtractionComplete = false;
    private boolean vertexLoadComplete = false;
    private boolean vertexValidationComplete = false;
    private boolean edgeIdWriteComplete = false;
    private boolean edgeLoadComplete = false;
    private boolean edgeValidationComplete = false;
    private boolean resumableLoad = false;
    private boolean resumeableLoadComplete = false;
    private long verticesWritten = 0L;
    private long edgesWritten = 0L;
    private long edgesInitial = 0L;
    private long verticesInitial = 0L;
    private int vertexPartitions = 0;
    private int edgePartitions = 0;

    public ProgressBar(final int intervalMillis) {
        this.intervalMillis = intervalMillis;
    }

    public void close() {
        synchronized (ProgressBar.class) {
            if (this.graph != null) {
                this.graph.close();
                this.graph = null;
            }
        }
    }

    public void initialize(final FireflyGraph graph, final boolean incrementalMode) {
        synchronized (ProgressBar.class) {
            this.graph = graph;
            if (incrementalMode) {
                final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata =
                        graph.fireflySummaryUpdater.getFireflyStatistics();
                verticesInitial = elementMetadata.totalVertexCount();
                edgesInitial = elementMetadata.totalEdgeCount();
            }
        }
    }

    public void setEdgeIdWriteComplete() {
        synchronized (ProgressBar.class) {
            this.edgeIdWriteComplete = true;
        }
    }

    public void setIsL2Mode(final boolean isL2Mode) {
        synchronized (ProgressBar.class) {
            this.isL2Mode = isL2Mode;
        }
    }

    public void setResumeableLoad() {
        synchronized (ProgressBar.class) {
            this.resumableLoad = true;
        }
    }

    public void setResumeableLoadComplete() {
        synchronized (ProgressBar.class) {
            this.resumeableLoadComplete = true;
        }
    }

    public void setVertexLoadComplete() {
        synchronized (ProgressBar.class) {
            this.vertexLoadComplete = true;
        }
    }

    public void setEdgeLoadComplete() {
        synchronized (ProgressBar.class) {
            this.edgeLoadComplete = true;
        }
    }

    public void setVertexValidationComplete() {
        synchronized (ProgressBar.class) {
            this.vertexValidationComplete = true;
        }
    }

    public void setEdgeValidationComplete() {
        synchronized (ProgressBar.class) {
            this.edgeValidationComplete = true;
        }
    }

    public void setPreflightCheckComplete() {
        synchronized (ProgressBar.class) {
            this.preflightCheckComplete = true;
        }
    }

    public void setSuperNodeExtractionComplete() {
        synchronized (ProgressBar.class) {
            this.superNodeExtractionComplete = true;
        }
    }

    private String getPreFlightCheckProgress() {
        if (!this.resumableLoad) {
            if (this.preflightCheckComplete) {
                return "\t\tPreflight check complete\n";
            } else {
                return "\t\tPreflight check in progress\n";
            }
        } else {
            return "\t\tPreflight check not started\n";
        }
    }

    public void setEdgePartitionCount(final int edgePartitions) {
        synchronized (ProgressBar.class) {
            this.edgePartitions = edgePartitions;
        }
    }

    public void setVertexPartitionCount(final int vertexPartitions) {
        synchronized (ProgressBar.class) {
            this.vertexPartitions = vertexPartitions;
        }
    }

    private String getPartitionProgress(final int totalPartitions, final int completePartitions) {
        if (graph == null || totalPartitions <= 0) {
            return null;
        } else {
            final double percentComplete = (double) completePartitions / totalPartitions;
            final int blocksComplete = (int) (percentComplete * 20);
            final String partitionProgress = IntStream.range(0, 20)
                    .mapToObj(i -> i < blocksComplete ? "■" : "□")
                    .collect(Collectors.joining());
            return "\t\t\t[" + partitionProgress + "]\n" +
                    "\t\t\t" + completePartitions + " of " + totalPartitions + " partitions complete (" +
                    String.format("%.2f", percentComplete * 100) + "%)\n";
        }
    }

    private String getVertexWritingProgress(final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata) {
        if (vertexLoadComplete) {
            return "\t\tVertex writing complete\n" +
                    "\t\t\tTotal of " + (elementMetadata.totalVertexCount() - verticesInitial) + " vertices have been successfully written\n";
        } else if (superNodeExtractionComplete) {
            if (verticesWritten == 0) {
                updateAndGetDeltaVertexCount(elementMetadata);
                return "\t\tVertex writing in progress\n";
            } else {
                final long delta = updateAndGetDeltaVertexCount(elementMetadata);
                final String output = "\t\tVertex writing in progress\n" +
                        "\t\t\tWriting " + delta / (intervalMillis / 1000) + " vertices per second\n" +
                        "\t\t\tTotal of " + verticesWritten + " vertices have been successfully written\n";
                final int totalPartitions = vertexPartitions;
                final int completePartitions = RecoveryUtil.completedVertexPartitions(graph.getBaseGraph()).size();
                final String partitionProgress = getPartitionProgress(totalPartitions, completePartitions);
                return output + (partitionProgress == null ? "" : partitionProgress);
            }
        } else {
            return "\t\tVertex writing not started\n";
        }
    }

    private long updateAndGetDeltaVertexCount(final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata) {
        final long totalVertexCount = elementMetadata.totalVertexCount() - verticesInitial;
        final long delta = totalVertexCount - verticesWritten;
        verticesWritten = totalVertexCount;
        return delta;
    }

    private String getEdgeWritingProgress(final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata) {
        if (edgeLoadComplete) {
            return "\t\tEdge writing complete\n" +
                    "\t\t\tTotal of " + (elementMetadata.totalEdgeCount() - edgesInitial) + " edges have been successfully written\n";
        } else if (vertexValidationComplete) {
            if (edgesWritten == 0) {
                updateAndGetDeltaEdgeCount(elementMetadata);
                return "\t\tEdge writing in progress\n";
            } else {
                final long delta = updateAndGetDeltaEdgeCount(elementMetadata);
                final String output = "\t\tEdge writing in progress\n" +
                        "\t\t\tWriting " + delta / (intervalMillis / 1000) + " edges per second\n" +
                        "\t\t\tTotal of " + edgesWritten + " edges have been successfully written\n";
                final int totalPartitions = edgePartitions;
                final int completePartitions = RecoveryUtil.completedEdgePartitions(graph.getBaseGraph()).size();
                final String partitionProgress = getPartitionProgress(totalPartitions, completePartitions);
                return output + (partitionProgress == null ? "" : partitionProgress);
            }
        } else {
            return "\t\tEdge writing not started\n";
        }
    }

    private long updateAndGetDeltaEdgeCount(final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata) {
        final long totalEdgeCount = elementMetadata.totalEdgeCount() - edgesInitial;
        final long delta = totalEdgeCount - edgesWritten;
        edgesWritten = totalEdgeCount;
        return delta;
    }

    private String getVertexValidationProgress() {
        if (vertexValidationComplete) {
            return "\t\tVertex validation complete\n";
        } else if (vertexLoadComplete) {
            return "\t\tVertex validation in progress\n";
        } else {
            return "\t\tVertex validation not started\n";
        }
    }

    private String getEdgeValidationProgress() {
        if (edgeValidationComplete) {
            return "\t\tEdge validation complete\n";
        } else if (edgeLoadComplete) {
            return "\t\tEdge validation in progress\n";
        } else {
            return "\t\tEdge validation not started\n";
        }
    }

    private String getSuperNodeExtractionProgress() {
        if (superNodeExtractionComplete) {
            return "\t\tSupernode extraction complete\n";
        } else if (edgeIdWriteComplete) {
            return "\t\tSupernode extraction in progress\n";
        } else {
            return "\t\tSupernode extraction not started\n";
        }
    }

    private String getEdgeIdProgress() {
        if (edgeIdWriteComplete) {
            return "\t\tTemp data writing complete\n";
        } else if (preflightCheckComplete) {
            return "\t\tTemp data writing in progress\n";
        } else {
            return "\t\tTemp data writing not started\n";
        }
    }

    private void printProgress() {
        synchronized (ProgressBar.class) {
            try {
                if (graph == null) {
                    return;
                }
                final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata = graph.fireflySummaryUpdater.getFireflyStatistics();
                LOGGER.info("\n\tBulk Loader Progress:\n" +
                        getResumeableLoadProgress() +
                        getPreFlightCheckProgress() +
                        getEdgeIdProgress() +
                        getSuperNodeExtractionProgress() +
                        getVertexWritingProgress(elementMetadata) +
                        getVertexValidationProgress() +
                        getEdgeWritingProgress(elementMetadata) +
                        getEdgeValidationProgress() +
                        JVMMemoryStats());
            } catch (final Exception e) {
                LOGGER.error("Error occurred when grabbing metadata information for progress bar: ", e);
            }
        }
    }

    public String getResumeableLoadProgress() {
        if (resumableLoad) {
            if (resumeableLoadComplete) {
                return "\t\tResuming load complete\n";
            } else {
                return "\t\tResuming load in progress\n";
            }
        } else {
            return "";
        }
    }

    public String JVMMemoryStats() {
        if (isL2Mode) {
            final Runtime javaRuntime = Runtime.getRuntime();
            final long maxMemory = javaRuntime.maxMemory() / (1024 * 1024 * 1024);
            final long totalMemory = javaRuntime.totalMemory() / (1024 * 1024 * 1024);
            final long freeMemory = javaRuntime.freeMemory() / (1024 * 1024 * 1024);
            return String.format("\tJVM Memory Stats: max/total/free memory (GB) %s/%s/%s",
                    maxMemory, totalMemory, freeMemory);
        } else {
            // In L3 mode, this information isn't useful. It is only meaningful in L2.
            return "";
        }
    }

    @Override
    public void run() {
        System.out.println("PROGRESS BAR");
        printProgress();
    }
}
