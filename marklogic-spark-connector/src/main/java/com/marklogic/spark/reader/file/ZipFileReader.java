/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.reader.document.DocumentRowBuilder;
import org.apache.commons.crypto.utils.IoUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


public class ZipFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ZipFileReader.class);

    private final FilePartition filePartition;
    private final FileContext fileContext;
    private final StreamingMode streamingMode;

    private int nextFilePathIndex;
    private String currentFilePath;
    private ZipInputStream currentZipInputStream;
    private ZipEntry currentZipEntry;

    ZipFileReader(FilePartition filePartition, FileContext fileContext) {
        this(filePartition, fileContext, fileContext.isStreamingFiles() ? StreamingMode.STREAM_DURING_READER_PHASE : null);
    }

    public ZipFileReader(FilePartition filePartition, FileContext fileContext, StreamingMode streamingMode) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
        this.streamingMode = streamingMode;
        openNextFile();
    }

    @Override
    public boolean next() {
        if (StreamingMode.STREAM_DURING_READER_PHASE.equals(this.streamingMode)) {
            return nextWhileStreamingDuringReaderPhase();
        }

        try {
            currentZipEntry = FileUtil.findNextFileEntry(currentZipInputStream);
        } catch (IOException e) {
            throw new ConnectorException(String.format(
                "Unable to read from zip file %s; cause: %s", currentFilePath, e.getMessage()), e);
        }

        if (currentZipEntry != null) {
            return true;
        }
        close();
        if (nextFilePathIndex == filePartition.getPaths().size()) {
            return false;
        }
        openNextFile();
        return next();
    }

    @Override
    public InternalRow get() {
        if (StreamingMode.STREAM_DURING_READER_PHASE.equals(this.streamingMode)) {
            return buildRowForZipFile();
        }

        String zipEntryName = currentZipEntry.getName();
        if (logger.isTraceEnabled()) {
            logger.trace("Reading zip entry {} from zip file {}.", zipEntryName, this.currentFilePath);
        }
        String uri = zipEntryName.startsWith("/") ?
            this.currentFilePath + zipEntryName :
            this.currentFilePath + "/" + zipEntryName;

        Object content = StreamingMode.STREAM_DURING_WRITER_PHASE.equals(this.streamingMode) ? null
            : ByteArray.concat(readZipEntry());

        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri), content, null, null, null, null, null, null
        });
    }

    /**
     * Exposed for {@code ZipFileIterator} to be able to read from the zip stream when it produces a set of
     * document inputs.
     *
     * @return a {@code InputStreamHandle} to avoid reading a content zip entry into memory.
     */
    public InputStreamHandle getContentHandleForCurrentZipEntry() {
        return new InputStreamHandle(currentZipInputStream);
    }

    @Override
    public void close() {
        IoUtils.closeQuietly(this.currentZipInputStream);
    }

    /**
     * Implementation of {@code next()} while streaming during the reader phase, where we don't want to actually read
     * anything from a zip file. We just want to build a row per zip file.
     */
    private boolean nextWhileStreamingDuringReaderPhase() {
        if (currentFilePath != null) {
            return true;
        }
        if (nextFilePathIndex >= filePartition.getPaths().size()) {
            return false;
        }
        openNextFile();
        return true;
    }

    private void openNextFile() {
        this.currentFilePath = filePartition.getPaths().get(nextFilePathIndex);
        nextFilePathIndex++;
        this.currentZipInputStream = new ZipInputStream(fileContext.openFile(this.currentFilePath));
    }

    private byte[] readZipEntry() {
        try {
            return fileContext.readBytes(currentZipInputStream);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read from zip file at %s; cause: %s",
                this.currentFilePath, e.getMessage()), e);
        }
    }

    private InternalRow buildRowForZipFile() {
        byte[] serializedFileContext = FileUtil.serializeFileContext(fileContext, currentFilePath);
        InternalRow row = new DocumentRowBuilder(new ArrayList<>())
            .withUri(this.currentFilePath)
            .withContent(serializedFileContext)
            .buildRow();
        this.currentFilePath = null;
        return row;
    }
}
