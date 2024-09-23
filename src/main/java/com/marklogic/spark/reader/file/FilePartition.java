/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import org.apache.spark.sql.connector.read.InputPartition;

import java.util.List;

public class FilePartition implements InputPartition {

    static final long serialVersionUID = 1;

    private final List<String> paths;

    public FilePartition(List<String> paths) {
        this.paths = paths;
    }

    public List<String> getPaths() {
        return paths;
    }

    @Override
    public String toString() {
        return this.paths.toString();
    }
}
