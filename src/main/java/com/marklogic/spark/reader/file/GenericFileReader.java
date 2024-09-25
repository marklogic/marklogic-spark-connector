/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;

/**
 * "Generic" = read each file as-is with no special processing.
 */
class GenericFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;
    private final boolean isStreaming;

    private InternalRow nextRowToReturn;
    private int filePathIndex;

    GenericFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
        this.isStreaming = "true".equalsIgnoreCase(fileContext.getStringOption(Options.STREAM_FILES));
    }

    @Override
    public boolean next() {
        if (filePathIndex >= filePartition.getPaths().size()) {
            return false;
        }

        final String path = fileContext.getDecodedFilePath(filePartition, filePathIndex);
        filePathIndex++;
        try {
            byte[] content = this.isStreaming ? serializeFileContext() : readFileIntoByteArray(path);

            nextRowToReturn = new GenericInternalRow(new Object[]{
                UTF8String.fromString(path),
                ByteArray.concat(content),
                null, null, null, null, null, null
            });
        } catch (Exception ex) {
            String message = String.format("Unable to read file at %s; cause: %s", path, ex.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, ex);
            }
            Util.MAIN_LOGGER.warn(message);
            return next();
        }
        return true;
    }

    @Override
    public InternalRow get() {
        return nextRowToReturn;
    }

    @Override
    public void close() throws IOException {
        // Nothing to close.
    }

    private byte[] readFileIntoByteArray(String path) throws IOException {
        try (InputStream inputStream = fileContext.openFile(path)) {
            return fileContext.readBytes(inputStream);
        }
    }

    private byte[] serializeFileContext() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(fileContext);
        }
        return baos.toByteArray();
    }
}
