package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;

class QuadsFileReader extends AbstractRdfFileReader implements PartitionReader<InternalRow> {

    private final QuadStreamReader quadStreamReader;

    QuadsFileReader(FilePartition partition, FileContext fileContext) {
        super(partition);
        try {
            this.inputStream = openStream(partition, fileContext);
            this.quadStreamReader = new QuadStreamReader(partition.getPath(), inputStream);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read file at %s; cause: %s", partition.getPath(), e.getMessage()), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        return quadStreamReader.hasNext();
    }

    @Override
    public InternalRow get() {
        return quadStreamReader.get();
    }
}
