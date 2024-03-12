package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.InputStream;

class AggregateXmlFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private InputStream inputStream;
    private AggregateXmlSplitter aggregateXMLSplitter;
    private InternalRow nextRowToReturn;

    AggregateXmlFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
    }

    @Override
    public boolean next() {
        if (aggregateXMLSplitter == null && !initializeAggregateXMLSplitter()) {
            return false;
        }

        // Iterate until either a valid element is found or we run out of elements.
        while (true) {
            try {
                if (!this.aggregateXMLSplitter.hasNext()) {
                    return false;
                }
            } catch (ConnectorException ex) {
                if (fileContext.isReadAbortOnFailure()) {
                    throw ex;
                }
                Util.MAIN_LOGGER.warn(ex.getMessage());
                return false;
            }

            try {
                nextRowToReturn = this.aggregateXMLSplitter.nextRow(filePartition.getPath());
                return true;
            } catch (RuntimeException ex) {
                // Error is expected to be friendly already.
                if (fileContext.isReadAbortOnFailure()) {
                    throw ex;
                }
                Util.MAIN_LOGGER.warn(ex.getMessage());
            }
        }
    }

    @Override
    public InternalRow get() {
        return nextRowToReturn;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(this.inputStream);
    }

    private boolean initializeAggregateXMLSplitter() {
        try {
            this.inputStream = fileContext.open(filePartition);
            String identifierForError = "file " + filePartition.getPath();
            this.aggregateXMLSplitter = new AggregateXmlSplitter(identifierForError, this.inputStream, fileContext.getProperties());
            return true;
        } catch (ConnectorException ex) {
            if (fileContext.isReadAbortOnFailure()) {
                throw ex;
            }
            Util.MAIN_LOGGER.warn(ex.getMessage());
            return false;
        } catch (Exception ex) {
            String message = String.format("Unable to read file at %s; cause: %s", filePartition.getPath(), ex.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, ex);
            }
            Util.MAIN_LOGGER.warn(ex.getMessage());
            return false;
        }
    }
}
