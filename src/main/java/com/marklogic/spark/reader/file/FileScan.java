package com.marklogic.spark.reader.file;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;
import org.apache.spark.sql.types.StructType;

class FileScan implements Scan {

    private PartitioningAwareFileIndex fileIndex;

    FileScan(PartitioningAwareFileIndex fileIndex) {
        this.fileIndex = fileIndex;
    }

    @Override
    public StructType readSchema() {
        return FileRowSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        return new FileBatch(fileIndex);
    }
}
