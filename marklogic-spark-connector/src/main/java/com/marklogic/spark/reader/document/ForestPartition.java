/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.client.datamovement.Forest;
import org.apache.spark.sql.connector.read.InputPartition;

class ForestPartition implements InputPartition {

    static final long serialVersionUID = 1;

    private final String forestName;
    private final String host;
    private final long serverTimestamp;
    private final Long offsetStart;
    private final Long offsetEnd;

    ForestPartition(Forest forest, long serverTimestamp, Long offsetStart, Long offsetEnd) {
        this.forestName = forest.getForestName();
        this.host = forest.getHost();
        this.serverTimestamp = serverTimestamp;
        this.offsetStart = offsetStart;
        this.offsetEnd = offsetEnd;
    }

    String getForestName() {
        return forestName;
    }

    String getHost() {
        return host;
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
