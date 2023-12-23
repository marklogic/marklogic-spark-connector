package com.marklogic.spark.reader.file;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;

class FileScanBuilder implements ScanBuilder {

    private PartitioningAwareFileIndex fileIndex;

    FileScanBuilder(PartitioningAwareFileIndex fileIndex) {
        this.fileIndex = fileIndex;
    }

    @Override
    public Scan build() {
        return new FileScan(fileIndex);
    }
}
