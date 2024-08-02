/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reduces some duplication across RdfFileReader and QuadsFileReader.
 */
class RdfFileReader implements PartitionReader<InternalRow> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private String currentFilePath;
    private RdfStreamReader currentRdfStreamReader;
    private InputStream currentInputStream;
    private int nextFilePathIndex;

    RdfFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
    }

    @Override
    public boolean next() {
        try {
            if (currentRdfStreamReader == null) {
                if (nextFilePathIndex >= this.filePartition.getPaths().size()) {
                    return false;
                }
                if (!initializeRdfStreamReader()) {
                    return next();
                }
            }
            if (currentRdfStreamReader.hasNext()) {
                return true;
            }
            currentRdfStreamReader = null;
            return next();
        } catch (ConnectorException ex) {
            if (fileContext.isReadAbortOnFailure()) {
                throw ex;
            }
            Util.MAIN_LOGGER.warn(ex.getMessage());
            return next();
        } catch (Exception ex) {
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(String.format("Unable to process RDF file %s, cause: %s", currentFilePath, ex.getMessage()), ex);
            }
            Util.MAIN_LOGGER.warn(ex.getMessage());
            return next();
        }
    }

    @Override
    public InternalRow get() {
        return currentRdfStreamReader.get();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.currentInputStream);
    }

    private boolean initializeRdfStreamReader() {
        this.currentFilePath = this.filePartition.getPaths().get(nextFilePathIndex);
        if (logger.isDebugEnabled()) {
            logger.debug("Reading file {}", this.currentFilePath);
        }
        try {
            this.currentInputStream = fileContext.openFile(this.currentFilePath);
            this.currentRdfStreamReader = RdfUtil.isQuadsFile(this.currentFilePath) ?
                new QuadStreamReader(this.currentFilePath, this.currentInputStream) :
                new TripleStreamReader(this.currentFilePath, this.currentInputStream);
            this.nextFilePathIndex++;
            return true;
        } catch (Exception e) {
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(String.format(
                    "Unable to read file at %s; cause: %s", this.currentFilePath, e.getMessage()), e);
            }
            Util.MAIN_LOGGER.warn("Unable to read file at {}; cause: {}", this.currentFilePath, e.getMessage());
            return false;
        }
    }
}
