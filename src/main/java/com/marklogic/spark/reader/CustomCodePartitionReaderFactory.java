package com.marklogic.spark.reader;

import com.marklogic.spark.CustomCodeContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

class CustomCodePartitionReaderFactory implements PartitionReaderFactory {

    private CustomCodeContext customCodeContext;

    public CustomCodePartitionReaderFactory(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new CustomCodePartitionReader(customCodeContext, ((CustomCodePartition) partition).getBatchId());
    }
}
