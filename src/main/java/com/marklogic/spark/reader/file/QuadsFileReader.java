package com.marklogic.spark.reader.file;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.InputStream;

class QuadsFileReader extends AbstractRdfFileReader implements PartitionReader<InternalRow> {

    QuadsFileReader(FilePartition partition, FileContext fileContext) {
        super(partition, fileContext);
    }

    @Override
    protected RdfStreamReader initializeRdfStreamReader(String path, InputStream inputStream) {
        return new QuadStreamReader(path, inputStream);
    }
}
