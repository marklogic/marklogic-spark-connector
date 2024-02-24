package com.marklogic.spark.writer;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Set;

public class CommitMessage implements WriterCommitMessage {

    private final int successItemCount;
    private final int failedItemCount;
    private final int partitionId;
    private final long taskId;
    private final long epochId;
    private final Set<String> graphs;

    /**
     * @param successItemCount
     * @param failedItemCount
     * @param partitionId
     * @param taskId
     * @param epochId          only populated when using Spark streaming
     * @param graphs           zero or more MarkLogic Semantics graph names, each of which is associated with a
     *                         graph document in MarkLogic that must be created after all the documents have been
     *                         written.
     */
    public CommitMessage(int successItemCount, int failedItemCount, int partitionId, long taskId, long epochId, Set<String> graphs) {
        this.successItemCount = successItemCount;
        this.failedItemCount = failedItemCount;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.graphs = graphs;
    }

    public int getSuccessItemCount() {
        return successItemCount;
    }

    public int getFailedItemCount() {
        return failedItemCount;
    }

    public Set<String> getGraphs() {
        return graphs;
    }

    @Override
    public String toString() {
        return epochId != 0L ?
            String.format("[partitionId: %d; taskId: %d; epochId: %d]; docCount: %d; failedItemCount: %d",
                partitionId, taskId, epochId, successItemCount, failedItemCount) :
            String.format("[partitionId: %d; taskId: %d]; docCount: %d; failedItemCount: %d",
                partitionId, taskId, successItemCount, failedItemCount);
    }
}
