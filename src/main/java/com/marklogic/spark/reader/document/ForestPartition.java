package com.marklogic.spark.reader.document;

import org.apache.spark.sql.connector.read.InputPartition;

class ForestPartition implements InputPartition {

    static final long serialVersionUID = 1;

    private String forestName;

    ForestPartition(String forestName) {
        this.forestName = forestName;
    }

    public String getForestName() {
        return forestName;
    }
}
