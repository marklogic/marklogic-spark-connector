package com.marklogic.spark.writer;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class CommitMessage implements WriterCommitMessage {

    private final int successItemCount;
    private final int failedItemCount;
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    public CommitMessage(int successItemCount, int failedItemCount, int partitionId, long taskId, long epochId) {
        this.successItemCount = successItemCount;
        this.failedItemCount = failedItemCount;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
    }

    public int getSuccessItemCount() {
        return successItemCount;
    }

    public int getFailedItemCount() {
        return failedItemCount;
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
