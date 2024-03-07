package com.marklogic.spark.reader.file;

import org.apache.spark.sql.connector.read.InputPartition;

class FilePartition implements InputPartition {

    static final long serialVersionUID = 1;

    private String path;

    FilePartition(String path) {
        this.path = path;
    }

    String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return path;
    }
}
