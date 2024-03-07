package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.InputStream;

class AggregateXMLFileReader implements PartitionReader<InternalRow> {

    private final String path;
    private final InputStream inputStream;
    private final AggregateXMLSplitter aggregateXMLSplitter;

    AggregateXMLFileReader(FilePartition filePartition, FileContext fileContext) {
        this.path = filePartition.getPath();
        this.inputStream = makeInputStream(filePartition, fileContext);

        String identifierForError = "file " + filePartition.getPath();
        try {
            this.aggregateXMLSplitter = new AggregateXMLSplitter(identifierForError, this.inputStream, fileContext.getProperties());
        } catch (Exception e) {
            // Interestingly, this won't fail if the file is malformed or not XML. It's only when we try to get the
            // first element.
            String message = String.format("Unable to read file at %s; cause: %s", filePartition.getPath(), e.getMessage());
            throw new ConnectorException(message, e);
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

    // Protected so that it can be overridden for gzipped files.
    protected InputStream makeInputStream(FilePartition filePartition, FileContext fileContext) {
        // Contrary to writing files, testing has shown no difference in performance with using e.g. FileInputStream
        // instead of fileSystem.open when fileSystem is a LocalFileSystem.
        return fileContext.open(filePartition);
    }
}
