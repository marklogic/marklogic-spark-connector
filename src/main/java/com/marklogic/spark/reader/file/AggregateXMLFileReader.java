package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Map;

class AggregateXMLFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(AggregateXMLFileReader.class);

    private final String path;
    private final InputStream inputStream;
    private final AggregateXMLSplitter aggregateXMLSplitter;

    AggregateXMLFileReader(FilePartition partition, Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        if (logger.isTraceEnabled()) {
            logger.trace("Reading path: {}", partition.getPath());
        }
        this.path = partition.getPath();
        Path hadoopPath = new Path(this.path);

        try {
            // Contrary to writing files, testing has shown no difference in performance with using e.g. FileInputStream
            // instead of fileSystem.open when fileSystem is a LocalFileSystem.
            this.inputStream = hadoopPath.getFileSystem(hadoopConfiguration.value()).open(hadoopPath);
            String identifierForError = "file " + hadoopPath;
            this.aggregateXMLSplitter = new AggregateXMLSplitter(identifierForError, this.inputStream, properties);
        } catch (Exception e) {
            // Interestingly, this won't fail if the file is malformed or not XML. It's only when we try to get the
            // first element.
            throw new ConnectorException(String.format("Unable to read file %s", hadoopPath), e);
        }

    }

    @Override
    public boolean next() {
        try {
            return this.aggregateXMLSplitter.hasNext();
        } catch (RuntimeException e) {
            String message = String.format("Unable to read XML from %s; cause: %s", this.path, e.getMessage());
            throw new ConnectorException(message, e);
        }
    }

    @Override
    public InternalRow get() {
        return this.aggregateXMLSplitter.nextRow(this.path);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(this.inputStream);
    }
}
