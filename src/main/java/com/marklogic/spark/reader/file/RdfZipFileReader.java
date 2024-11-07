/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class RdfZipFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(RdfZipFileReader.class);

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private String currentFilePath;
    private CustomZipInputStream currentZipInputStream;
    private RdfStreamReader currentRdfStreamReader;
    private int nextFilePathIndex;

    RdfZipFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
    }

    @Override
    public boolean next() {
        try {
            // If we've got a reader and a row, we're good to go.
            if (currentRdfStreamReader != null && currentRdfStreamReader.hasNext()) {
                return true;
            }

            // In case this was not null but just out of entries, set it to null.
            currentRdfStreamReader = null;

            // If we have a zip open, look for the next RDF entry to process.
            if (currentZipInputStream != null) {
                ZipEntry zipEntry = FileUtil.findNextFileEntry(currentZipInputStream);
                if (zipEntry == null) {
                    // If the zip is empty, go to the next file.
                    currentZipInputStream = null;
                    return next();
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("Reading entry {} in {}", zipEntry.getName(), this.currentFilePath);
                }
                this.currentRdfStreamReader = RdfUtil.isQuadsFile(zipEntry.getName()) ?
                    new QuadStreamReader(zipEntry.getName(), currentZipInputStream) :
                    new TripleStreamReader(zipEntry.getName(), currentZipInputStream);
                return next();
            }

            // If we get here, it's time for the next file, if one exists.
            if (nextFilePathIndex >= filePartition.getPaths().size()) {
                return false;
            }

            // Open up the next zip.
            this.currentFilePath = filePartition.getPaths().get(nextFilePathIndex);
            nextFilePathIndex++;
            this.currentZipInputStream = new CustomZipInputStream(fileContext.openFile(currentFilePath));
            return next();
        } catch (Exception ex) {
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(String.format("Unable to process zip file at %s, cause: %s", currentFilePath, ex.getMessage()), ex);
            }
            Util.MAIN_LOGGER.warn(ex.getMessage());
            return next();
        }
    }

    @Override
    public InternalRow get() {
        return this.currentRdfStreamReader.get();
    }

    @Override
    public void close() throws IOException {
        if (this.currentZipInputStream != null) {
            this.currentZipInputStream.readyToClose = true;
            IOUtils.closeQuietly(this.currentZipInputStream);
        }
    }

    /**
     * Per https://jena.apache.org/documentation/io/rdf-input.html#iterating-over-parser-output , Jena will call
     * close() on an iterator after reading all the triples/quads. This results in the ZipInputStream being closed,
     * which prevents any additional entries from being read.
     * <p>
     * We know we only want to close the stream when Spark calls close() or abort(). So this modifies the close() method
     * to only close after a boolean has been flipped to true.
     */
    private static class CustomZipInputStream extends ZipInputStream {

        private boolean readyToClose = false;

        public CustomZipInputStream(@NotNull InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
            if (readyToClose) {
                super.close();
            }
        }
    }
}
