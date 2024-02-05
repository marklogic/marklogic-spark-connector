package com.marklogic.spark.reader.document;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * Defines a range of pages where the first page begins at {@code offsetStart} in the overall set of search results,
 * and ends at {@code offsetEnd}, which may exceed the total number of search results.
 */
class PageRangePartition implements InputPartition {

    private final long offsetStart;
    private final long offsetEnd;
    private final long serverTimestamp;

    PageRangePartition(long offsetStart, long offsetEnd, long serverTimestamp) {
        this.offsetStart = offsetStart;
        this.offsetEnd = offsetEnd;
        this.serverTimestamp = serverTimestamp;
    }

    long getOffsetStart() {
        return offsetStart;
    }

    long getOffsetEnd() {
        return offsetEnd;
    }

    long getServerTimestamp() {
        return serverTimestamp;
    }

    long getLength() {
        return offsetEnd - offsetStart + 1;
    }

    @Override
    public String toString() {
        return String.format("[%d:%d]", offsetStart, offsetEnd);
    }
}
