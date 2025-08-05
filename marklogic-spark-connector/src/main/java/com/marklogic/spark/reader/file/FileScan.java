/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

class FileScan implements Scan {

    private final Map<String, String> properties;
    private final PartitioningAwareFileIndex fileIndex;

    FileScan(Map<String, String> properties, PartitioningAwareFileIndex fileIndex) {
        this.properties = properties;
        this.fileIndex = fileIndex;
    }

    @Override
    public StructType readSchema() {
        return DocumentRowSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        return new FileBatch(properties, fileIndex);
    }
}
