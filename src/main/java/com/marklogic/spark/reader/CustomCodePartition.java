package com.marklogic.spark.reader;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

class CustomCodePartition implements InputPartition, Serializable {

    final static long serialVersionUID = 1;

    private String batchId;

    /**
     * Constructor for normal reading, where all rows will be returned in a single call to MarkLogic by a single reader.
     */
    public CustomCodePartition() {
    }

    /**
     * Constructor used for streaming reads, when a call is made to the reader (and thus to MarkLogic) for the given
     * batch ID.
     *
     * @param batchId
     */
    public CustomCodePartition(String batchId) {
        this.batchId = batchId;
    }

    public String getBatchId() {
        return batchId;
    }
}
