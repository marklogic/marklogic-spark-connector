package com.marklogic.spark.reader.file;

import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Functions the same as reading an aggregate XML file - it just needs to wrap the input stream in a
 * {@code GZIPInputStream} first.
 */
class GZIPAggregateXMLFileReader extends AggregateXMLFileReader {

    GZIPAggregateXMLFileReader(FilePartition partition, Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        super(partition, properties, hadoopConfiguration);
    }

    @Override
    protected InputStream makeInputStream(Path path, SerializableConfiguration hadoopConfiguration) throws IOException {
        return new GZIPInputStream(path.getFileSystem(hadoopConfiguration.value()).open(path));
    }
}
