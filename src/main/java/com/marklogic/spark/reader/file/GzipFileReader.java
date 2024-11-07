/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.client.io.InputStreamHandle;
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
public class GzipFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;
    private final StreamingMode streamingMode;

    private int nextFilePathIndex;
    private InternalRow rowToReturn;

    // Only set if streaming during the writer phase.
    private InputStreamHandle streamingContentHandle;

    public GzipFileReader(FilePartition filePartition, FileContext fileContext) {
        this(filePartition, fileContext, fileContext.isStreamingFiles() ? StreamingMode.STREAM_DURING_READER_PHASE : null);
    }

    public GzipFileReader(FilePartition filePartition, FileContext fileContext, StreamingMode streamingMode) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
        this.streamingMode = streamingMode;
    }

    @Override
    public boolean next() {
        if (nextFilePathIndex >= filePartition.getPaths().size()) {
            return false;
        }

        String currentFilePath = filePartition.getPaths().get(nextFilePathIndex);
        nextFilePathIndex++;
        String uri = makeURI(currentFilePath);

        Object contentValue;
        if (StreamingMode.STREAM_DURING_READER_PHASE.equals(streamingMode)) {
            contentValue = FileUtil.serializeFileContext(fileContext, currentFilePath);
            uri = currentFilePath;
        } else if (StreamingMode.STREAM_DURING_WRITER_PHASE.equals(streamingMode)) {
            streamingContentHandle = new InputStreamHandle(fileContext.openFile(currentFilePath));
            contentValue = null;
        } else {
            InputStream gzipInputStream = null;
            try {
                gzipInputStream = fileContext.openFile(currentFilePath);
                byte[] content = extractGZIPContents(currentFilePath, gzipInputStream);
                contentValue = ByteArray.concat(content);
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

        this.rowToReturn = new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri), contentValue, null, null, null, null, null, null
        });
        return true;
    }

    @Override
    public InternalRow get() {
        return rowToReturn;
    }

    public InputStreamHandle getStreamingContentHandle() {
        return streamingContentHandle;
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
