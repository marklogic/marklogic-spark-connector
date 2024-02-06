package com.marklogic.spark.reader.document;

import org.apache.spark.sql.connector.read.InputPartition;

class ForestPartition implements InputPartition {

    static final long serialVersionUID = 1;

    private final String forestName;
    private final long serverTimestamp;
    private final Long offsetStart;
    private final Long offsetEnd;

    ForestPartition(String forestName, long serverTimestamp, Long offsetStart, Long offsetEnd) {
        this.forestName = forestName;
        this.serverTimestamp = serverTimestamp;
        this.offsetStart = offsetStart;
        this.offsetEnd = offsetEnd;
    }

    String getForestName() {
        return forestName;
    }

    long getServerTimestamp() {
        return serverTimestamp;
    }

    Long getOffsetStart() {
        return offsetStart;
    }

    Long getOffsetEnd() {
        return offsetEnd;
    }

    @Override
    public String toString() {
        return String.format("[%s; %d; %d]", forestName, offsetStart, offsetEnd);
    }
}
