package com.marklogic.spark.reader.file;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex;

public class FileScanBuilder implements ScanBuilder {

    private PartitioningAwareFileIndex fileIndex;

    public FileScanBuilder(PartitioningAwareFileIndex fileIndex) {
        this.fileIndex = fileIndex;
    }

    @Override
    public Scan build() {
        return new FileScan(fileIndex);
    }
}
