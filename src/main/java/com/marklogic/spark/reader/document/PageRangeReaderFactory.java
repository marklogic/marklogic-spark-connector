package com.marklogic.spark.reader.document;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class PageRangeReaderFactory implements PartitionReaderFactory {

    static final long serialVersionUID = 1;

    private DocumentContext documentContext;

    PageRangeReaderFactory(DocumentContext documentContext) {
        this.documentContext = documentContext;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new PageRangeReader((PageRangePartition) partition, documentContext);
    }

}
