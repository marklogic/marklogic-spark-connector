package com.marklogic.spark.reader.document;

import com.marklogic.spark.Options;
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
        if (this.documentContext.hasOption(Options.READ_TRIPLES_COLLECTIONS)) {
            return new TriplesReader((ForestPartition) partition, documentContext);
        }
        return new ForestReader((ForestPartition) partition, documentContext);
    }
}
