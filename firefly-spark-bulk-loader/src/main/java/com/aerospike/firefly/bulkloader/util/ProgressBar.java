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
    private boolean vertexLoadComplete = false;
    private boolean vertexValidationComplete = false;
    private boolean superNodeExtractionComplete = false;
    private boolean edgeLoadComplete = false;
    private boolean edgeValidationComplete = false;
    private long vertexCount = 0L;
    private long edgeCount = 0L;
    private long startVertexTime = 0L;
    private long startEdgeTime = 0L;

    public ProgressBar() {
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

    public void setGraph(final FireflyGraph graph) {
        synchronized (ProgressBar.class) {
            this.graph = graph;
        }
    }

    public void setVertexCount(final long vertexCount) {
        synchronized (ProgressBar.class) {
            this.startVertexTime = System.currentTimeMillis();
            this.vertexCount = vertexCount;
        }
    }

    public void setEdgeCount(final long edgeCount) {
        synchronized (ProgressBar.class) {
            this.startEdgeTime = System.currentTimeMillis();
            this.edgeCount = edgeCount;
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

    public void setSuperNodeExtractionComplete() {
        synchronized (ProgressBar.class) {
            this.superNodeExtractionComplete = true;
        }
    }

    private String getPreFlightCheckProgress() {
        if (vertexCount == 0 && edgeCount == 0) {
            return "\t\tPreflight check in progress\n";
        } else {
            return "\t\tPreflight check complete\n";
        }
    }

    private String getVertexWritingProgress(final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata) {
        if (vertexLoadComplete) {
            return "\t\tVertex writing complete\n";
        } else if (vertexCount != 0 && vertexCount == elementMetadata.totalVertexCount()) {
            return "\t\tVertex writing progress\n" +
                    "\t\t\t[" + (getProgressBar((double) elementMetadata.totalVertexCount() /
                    (double) vertexCount)) + "] (" + elementMetadata.totalVertexCount() + "/" + vertexCount + ")\n";
        } else if (vertexCount != 0) {
            return "\t\tVertex writing progress\n" +
                    "\t\t\t[" + (getProgressBar((double) elementMetadata.totalVertexCount() /
                    (double) vertexCount)) + "] (" + elementMetadata.totalVertexCount() + "/" + vertexCount + ")\n" +
                    "\t\t\t" + getTimeRemaining(startVertexTime, elementMetadata.totalVertexCount(), vertexCount, "Vertex") + "\n";
        } else {
            return "\t\tVertex writing not started\n";
        }
    }

    private String getEdgeWritingProgress(final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata) {
        if (edgeLoadComplete) {
            return "\t\tEdge writing complete\n";
        } else if (edgeCount != 0 && edgeCount == elementMetadata.totalEdgeCount()) {
            return "\t\tEdge writing progress\n" +
                    "\t\t\t[" + (getProgressBar((double) elementMetadata.totalEdgeCount() /
                    (double) edgeCount)) + "] (" + elementMetadata.totalEdgeCount() + "/" + edgeCount + ")\n";
        } else if (edgeCount != 0) {
            return "\t\tEdge writing progress\n" +
                    "\t\t\t[" + (getProgressBar((double) elementMetadata.totalEdgeCount() /
                    (double) edgeCount)) + "] (" + elementMetadata.totalEdgeCount() + "/" + edgeCount + ")\n" +
                    "\t\t\t" + getTimeRemaining(startEdgeTime, elementMetadata.totalEdgeCount(), edgeCount, "Edge") + "\n";
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

    private String superNodeExtractionProgress() {
        if (superNodeExtractionComplete) {
            return "\t\tSupernode extraction complete\n";
        } else if (vertexValidationComplete) {
            return "\t\tSupernode extraction in progress\n";
        } else {
            return "\t\tSupernode extraction not started\n";
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
                        getPreFlightCheckProgress() +
                        getVertexWritingProgress(elementMetadata) +
                        getVertexValidationProgress() +
                        superNodeExtractionProgress() +
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
