package com.marklogic.spark.reader.document;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

class ForestReaderFactory implements PartitionReaderFactory {

    static final long serialVersionUID = 1;
    
    private DocumentContext documentContext;

    ForestReaderFactory(DocumentContext documentContext) {
        this.documentContext = documentContext;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new ForestReader((ForestPartition) partition, documentContext);
    }
}
