/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;

import java.util.Map;

public class FileScanBuilder implements ScanBuilder {

    private final Map<String, String> properties;
    private final PartitioningAwareFileIndex fileIndex;

    public FileScanBuilder(Map<String, String> properties, PartitioningAwareFileIndex fileIndex) {
        this.properties = properties;
        this.fileIndex = fileIndex;
    }

    @Override
    public Scan build() {
        return new FileScan(properties, fileIndex);
    }
}
