package com.marklogic.spark.reader;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

class CustomCodePartition implements InputPartition, Serializable {

    static final long serialVersionUID = 1;

    private String partition;

    public CustomCodePartition() {
    }

    public CustomCodePartition(String partition) {
        this.partition = partition;
    }

    public String getPartition() {
        return partition;
    }
}
