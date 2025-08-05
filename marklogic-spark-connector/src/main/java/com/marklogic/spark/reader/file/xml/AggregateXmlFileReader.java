/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file.xml;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.file.FileContext;
import com.marklogic.spark.reader.file.FilePartition;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.InputStream;

public class AggregateXmlFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private InputStream inputStream;
    private AggregateXmlSplitter aggregateXMLSplitter;
    private InternalRow nextRowToReturn;
    private int filePathIndex = 0;

    public AggregateXmlFileReader(FilePartition filePartition, FileContext fileContext) {
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
                    aggregateXMLSplitter = null;
                    filePathIndex++;
                    return next();
                }
            } catch (ConnectorException ex) {
                if (fileContext.isReadAbortOnFailure()) {
                    throw ex;
                }
                Util.MAIN_LOGGER.warn(ex.getMessage());
                aggregateXMLSplitter = null;
                filePathIndex++;
                return next();
            }

            try {
                String path = filePartition.getPaths().get(filePathIndex);
                nextRowToReturn = this.aggregateXMLSplitter.nextRow(path);
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
        if (filePathIndex >= filePartition.getPaths().size()) {
            return false;
        }

        final String filePath = filePartition.getPaths().get(filePathIndex);
        try {
            this.inputStream = fileContext.openFile(filePath);
            String identifierForError = "file " + filePath;
            this.aggregateXMLSplitter = new AggregateXmlSplitter(identifierForError, this.inputStream, fileContext);
            return true;
        } catch (ConnectorException ex) {
            if (fileContext.isReadAbortOnFailure()) {
                throw ex;
            }
            Util.MAIN_LOGGER.warn(ex.getMessage());
            filePathIndex++;
            return initializeAggregateXMLSplitter();
        } catch (Exception ex) {
            String message = String.format("Unable to read file at %s; cause: %s", filePath, ex.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, ex);
            }
            Util.MAIN_LOGGER.warn(ex.getMessage());
            filePathIndex++;
            return initializeAggregateXMLSplitter();
        }
    }
}
