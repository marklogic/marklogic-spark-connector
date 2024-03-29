package com.marklogic.spark.reader.document;

import com.marklogic.spark.reader.file.TripleRowSchema;
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
        return TripleRowSchema.SCHEMA.equals(documentContext.getSchema()) ?
            new TriplesReader((ForestPartition) partition, documentContext) :
            new ForestReader((ForestPartition) partition, documentContext);
    }
}
