package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.util.Map;

class TriplesFileReader extends AbstractRdfFileReader implements PartitionReader<InternalRow> {

    private final TripleStreamReader tripleStreamReader;

    TriplesFileReader(FilePartition partition, SerializableConfiguration hadoopConfiguration, Map<String, String> properties) {
        super(partition);
        final Path path = new Path(partition.getPath());
        try {
            this.inputStream = openStream(path, hadoopConfiguration, properties);
            this.tripleStreamReader = new TripleStreamReader(partition.getPath(), this.inputStream);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read RDF file at %s; cause: %s", path, e.getMessage()), e);
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
