package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;

class TriplesFileReader extends AbstractRdfFileReader implements PartitionReader<InternalRow> {

    private final TripleStreamReader tripleStreamReader;

    TriplesFileReader(FilePartition partition, FileContext fileContext) {
        super(partition);
        try {
            this.inputStream = openStream(partition, fileContext);
            this.tripleStreamReader = new TripleStreamReader(partition.getPath(), this.inputStream);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read RDF file at %s; cause: %s", partition.getPath(), e.getMessage()), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        return this.tripleStreamReader.hasNext();
    }

    @Override
    public InternalRow get() {
        return this.tripleStreamReader.get();
    }
}
