/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.InputStream;

/**
 * "Generic" = read each file as-is with no special processing.
 */
class GenericFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private InternalRow nextRowToReturn;
    private int filePathIndex;

    GenericFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
    }

    @Override
    public boolean next() {
        if (filePathIndex >= filePartition.getPaths().size()) {
            return false;
        }

        final String path = filePartition.getPaths().get(filePathIndex);
        filePathIndex++;
        try {
            try (InputStream inputStream = fileContext.openFile(path)) {
                byte[] content = fileContext.readBytes(inputStream);
                nextRowToReturn = new GenericInternalRow(new Object[]{
                    UTF8String.fromString(path),
                    ByteArray.concat(content),
                    null, null, null, null, null, null
                });
            }
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
}
