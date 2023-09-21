package com.aerospike.firefly.bulkloader.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.util.FireflyGraphSummaryUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProgressBar extends TimerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProgressBar.class);

    private FireflyGraph graph = null;
    private boolean preflightCheckComplete = false;
    private boolean superNodeExtractionComplete = false;
    private boolean vertexLoadComplete = false;
    private boolean vertexValidationComplete = false;

    private boolean edgeIdWriteComplete = false;
    private long startEdgeIdWrite = 0L;
    private boolean edgeLoadComplete = false;
    private boolean edgeValidationComplete = false;
    private long startVertexTime = 0L;
    private long startEdgeTime = 0L;

    public ProgressBar() {
    }

    public void close() {
        if (graph != null) {
            graph.close();
        }
    }

    public void setGraph(final FireflyGraph graph) {
        synchronized (ProgressBar.class) {
            this.graph = graph;
        }
    }

    public void setVertexLoadStart() {
        synchronized (ProgressBar.class) {
            this.startVertexTime = System.currentTimeMillis();
        }
    }

    public void setStartEdgeIdWrite() {
        synchronized (ProgressBar.class) {
            this.startEdgeIdWrite = System.currentTimeMillis();
        }
    }

    public void setEdgeIdWriteComplete() {
        synchronized (ProgressBar.class) {
            this.edgeIdWriteComplete = true;
        }
    }

    public void setEdgeLoadStart() {
        synchronized (ProgressBar.class) {
            this.startEdgeTime = System.currentTimeMillis();
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
        if (this.preflightCheckComplete) {
            return "\t\tPreflight check complete\n";
        } else {
            return "\t\tPreflight check in progress\n";
        }
    }

    private String getVertexWritingProgress(final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata) {
        if (vertexLoadComplete) {
            return "\t\tVertex writing complete\n";
        } else if (startVertexTime != 0) {
            long time = System.currentTimeMillis();
            if ((time - startVertexTime) / 1000 != 0) {
                return "\t\tVertex writing progress\n" +
                        "\t\t\tWriting " + elementMetadata.totalVertexCount() /
                        ((time - startVertexTime) / 1000) + " vertices per second\n";
            } else {
                return "\t\tVertex writing progress\n";
            }
        } else {
            return "\t\tVertex writing not started\n";
        }
    }

    private String getEdgeWritingProgress(final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata) {
        if (edgeLoadComplete) {
            return "\t\tEdge writing complete\n";
        } else if (startEdgeTime != 0) {
            long time = System.currentTimeMillis();
            if ((time - startEdgeTime) / 1000 != 0) {
                return "\t\tEdge writing progress\n" +
                        "\t\t\tWriting " + elementMetadata.totalEdgeCount() /
                        ((time - startEdgeTime) / 1000) + " edges per second\n";
            } else {
                return "\t\tEdge writing progress\n";
            }
        } else {
            return "\t\tEdge writing progress not started\n";
        }
    }

    private String getTimeRemaining(final long startTime, final long currentCount, final long totalCount, final String type) {
        final long currentTime = System.currentTimeMillis();
        final long timeElapsed = currentTime - startTime;
        if (currentCount == 0) {
            return type + " writing step time remaining: unknown";
        }
        final long seconds = (long) (((double) timeElapsed / (double) currentCount) * (totalCount - currentCount)) / 1000;

        long day = TimeUnit.SECONDS.toDays(seconds);
        long hours = TimeUnit.SECONDS.toHours(seconds) - (day * 24);
        long minute = TimeUnit.SECONDS.toMinutes(seconds) - (TimeUnit.SECONDS.toHours(seconds) * 60);
        long second = TimeUnit.SECONDS.toSeconds(seconds) - (TimeUnit.SECONDS.toMinutes(seconds) * 60);
        String timeRemaining = "";
        if (day > 0) {
            timeRemaining += day + " days ";
        }
        if (hours > 0) {
            timeRemaining += hours + " hours ";
        }
        if (minute > 0) {
            timeRemaining += minute + " minutes ";
        }
        if (second > 0) {
            timeRemaining += second + " seconds";
        }

        return "Estimated time remaining for step: " + timeRemaining;
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
        } else if (preflightCheckComplete) {
            return "\t\tSupernode extraction in progress\n";
        } else {
            return "\t\tSupernode extraction not started\n";
        }
    }

    private String getEdgeIdProgress() {
        if (edgeIdWriteComplete) {
            return "\t\tEdgeId writing complete\n";
        } else if (preflightCheckComplete) {
            return "\t\tEdgeId writing in progress\n";
        } else {
            return "\t\tEdgeId writing not started\n";
        }
    }

    private static String getProgressBar(final double percent) {
        final String block = "█";
        final int width = 50;
        final int progress = (int) (width * percent);
        String progressBlocks = IntStream.range(0, progress)
                .mapToObj(i -> block)
                .collect(Collectors.joining());
        String emptyBlocks = IntStream.range(0, width - progress)
                .mapToObj(i -> " ")
                .collect(Collectors.joining());
        return progressBlocks + emptyBlocks;

    }

    private void printProgress() {
        synchronized (ProgressBar.class) {
            try {
                if (graph == null) {
                    return;
                }
                final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata = graph.fireflySummaryUpdater.getFireflyStatistics();
                LOGGER.info("\n\tBulk Loader Progress:\n" +
                        getPreFlightCheckProgress() +
                        getEdgeIdProgress() +
                        getSuperNodeExtractionProgress() +
                        getVertexWritingProgress(elementMetadata) +
                        getVertexValidationProgress() +
                        getEdgeWritingProgress(elementMetadata) +
                        getEdgeValidationProgress());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        printProgress();
    }
}
