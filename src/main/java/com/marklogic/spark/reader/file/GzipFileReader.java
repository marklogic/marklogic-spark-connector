/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.InputStream;

/**
 * Expects to read a single gzipped file and return a single row. May expand the scope of this later to expect multiple
 * files and to thus return multiple rows.
 */
class GzipFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private int nextFilePathIndex;
    private InternalRow rowToReturn;

    GzipFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
    }

    @Override
    public boolean next() {
        if (nextFilePathIndex >= filePartition.getPaths().size()) {
            return false;
        }

        String currentFilePath = fileContext.getDecodedFilePath(filePartition, nextFilePathIndex);
        nextFilePathIndex++;
        InputStream gzipInputStream = null;
        try {
            gzipInputStream = fileContext.openFile(currentFilePath);
            byte[] content = extractGZIPContents(currentFilePath, gzipInputStream);
            String uri = makeURI(currentFilePath);
            this.rowToReturn = new GenericInternalRow(new Object[]{
                UTF8String.fromString(uri), ByteArray.concat(content),
                null, null, null, null, null, null
            });
            return true;
        } catch (RuntimeException ex) {
            if (fileContext.isReadAbortOnFailure()) {
                throw ex;
            }
            Util.MAIN_LOGGER.warn("Unable to read file at {}; cause: {}", currentFilePath, ex.getMessage());
            return next();
        } finally {
            IOUtils.closeQuietly(gzipInputStream);
        }
    }

    @Override
    public InternalRow get() {
        return rowToReturn;
    }

    @Override
    public void close() {
        // Nothing to close.
    }

    private byte[] extractGZIPContents(String currentFilePath, InputStream gzipInputStream) {
        try {
            return fileContext.readBytes(gzipInputStream);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read from gzip file at %s; cause: %s",
                currentFilePath, e.getMessage()), e);
        }
    }

    private String makeURI(String path) {
        // Copied from MLCP.
        return path.endsWith(".gzip") || path.endsWith(".gz") ?
            path.substring(0, path.lastIndexOf(".")) :
            path;
    }
}
