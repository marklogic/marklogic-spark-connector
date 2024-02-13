package com.marklogic.spark.writer;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class CommitMessage implements WriterCommitMessage {

    private final String action;
    private final int docCount;
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    public CommitMessage(String action, int docCount, int partitionId, long taskId, long epochId) {
        this.action = action;
        this.docCount = docCount;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
    }

    public String getAction() {
        return action;
    }

    public int getDocCount() {
        return docCount;
    }

    @Override
    public String toString() {
        return epochId != 0L ?
            String.format("[partitionId: %d; taskId: %d; epochId: %d]; docCount: %d", partitionId, taskId, epochId, docCount) :
            String.format("[partitionId: %d; taskId: %d]; docCount: %d", partitionId, taskId, docCount);
    }
}
