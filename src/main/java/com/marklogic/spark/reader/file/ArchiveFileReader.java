/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ArchiveFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;
    private final List<String> metadataCategories;
    private final StreamingMode streamingMode;

    private String currentFilePath;
    private ZipInputStream currentZipInputStream;
    private int nextFilePathIndex;
    private InternalRow nextRowToReturn;

    // Legacy = content first, then metadata.
    private Boolean isLegacyFormat;

    ArchiveFileReader(FilePartition filePartition, FileContext fileContext) {
        this(
            filePartition, fileContext,
            fileContext.isStreamingFiles() ? StreamingMode.STREAM_DURING_READER_PHASE : null
        );
    }

    public ArchiveFileReader(FilePartition filePartition, FileContext fileContext, StreamingMode streamingMode) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
        this.streamingMode = streamingMode;

        this.metadataCategories = new ArrayList<>();
        if (fileContext.hasOption(Options.READ_ARCHIVES_CATEGORIES)) {
            for (String category : fileContext.getStringOption(Options.READ_ARCHIVES_CATEGORIES).split(",")) {
                this.metadataCategories.add(category.toLowerCase());
            }
        }

        openNextFile();
    }

    @Override
    public boolean next() {
        if (StreamingMode.STREAM_DURING_READER_PHASE.equals(this.streamingMode)) {
            return nextWhileStreamingDuringReaderPhase();
        }

        try {
            ZipEntry nextZipEntry = FileUtil.findNextFileEntry(currentZipInputStream);
            if (nextZipEntry == null) {
                return openNextFileAndReadNextEntry();
            }

            if (isLegacyFormat == null) {
                isLegacyFormat = !nextZipEntry.getName().endsWith(".metadata");
                logArchiveFormat();
            }

            return isLegacyFormat ? readContentFollowedByMetadata(nextZipEntry) : readMetadataFollowedByContent();
        } catch (IOException e) {
            String message = String.format("Unable to read archive file at %s; cause: %s", this.currentFilePath, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, e);
            }
            Util.MAIN_LOGGER.warn(message);
            return openNextFileAndReadNextEntry();
        }
    }

    @Override
    public InternalRow get() {
        return StreamingMode.STREAM_DURING_READER_PHASE.equals(this.streamingMode) ?
            buildSingleRowForArchiveFile() :
            nextRowToReturn;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(this.currentZipInputStream);
    }

    /**
     * Exposed for {@code ArchiveFileIterator} to be able to read from the zip stream when it produces a set of
     * document inputs.
     *
     * @return a {@code InputStreamHandle} to avoid reading a content zip entry into memory.
     */
    public InputStreamHandle getContentHandleForCurrentZipEntry() {
        return new InputStreamHandle(currentZipInputStream);
    }

    private void logArchiveFormat() {
        if (Util.MAIN_LOGGER.isInfoEnabled() && isLegacyFormat) {
            Util.MAIN_LOGGER.info("Archive {} uses Flux 1.0 format, will read content and then metadata.", this.currentFilePath);
        }
        if (Util.MAIN_LOGGER.isDebugEnabled() && !isLegacyFormat.booleanValue()) {
            Util.MAIN_LOGGER.debug("Archive {} uses Flux 1.1+ format, will read metadata and then content.", this.currentFilePath);
        }
    }

    /**
     * Implementation of {@code next()} while streaming during the reader phase, where we don't want to actually read
     * anything from a zip file. We just want to build a row per zip file.
     *
     * @return
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

    /**
     * This is the Flux 1.0 "legacy" approach, where content was written first, followed by metadata. This does not
     * support streaming.
     */
    private boolean readContentFollowedByMetadata(ZipEntry contentZipEntry) throws IOException {
        byte[] content = fileContext.readBytes(currentZipInputStream);
        if (content == null || content.length == 0) {
            return openNextFileAndReadNextEntry();
        }

        final String zipEntryName = contentZipEntry.getName();
        byte[] metadataBytes = readMetadataEntry(zipEntryName);
        if (metadataBytes == null || metadataBytes.length == 0) {
            return openNextFileAndReadNextEntry();
        }

        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.fromBuffer(metadataBytes);
        this.nextRowToReturn = new DocumentRowBuilder(this.metadataCategories)
            .withUri(zipEntryName).withContent(content).withMetadata(metadata)
            .buildRow();
        return true;
    }

    /**
     * This is the Flux 1.1+ approach, where the metadata entry is written first. This supports streaming.
     * <p>
     * This is where we implement streaming-during-write-to-MarkLogic. We read the metadata entry as normal - good.
     * Then we build everything in our row except the content.
     */
    private boolean readMetadataFollowedByContent() throws IOException {
        byte[] metadataBytes = fileContext.readBytes(currentZipInputStream);
        if (metadataBytes == null || metadataBytes.length == 0) {
            return openNextFileAndReadNextEntry();
        }

        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.fromBuffer(metadataBytes);

        // We still do this to get the stream ready to read the next entry.
        ZipEntry contentZipEntry = FileUtil.findNextFileEntry(currentZipInputStream);

        DocumentRowBuilder rowBuilder = new DocumentRowBuilder(this.metadataCategories)
            .withUri(contentZipEntry.getName())
            .withMetadata(metadata);

        if (!StreamingMode.STREAM_DURING_WRITER_PHASE.equals(this.streamingMode)) {
            byte[] content = fileContext.readBytes(currentZipInputStream);
            rowBuilder = rowBuilder.withContent(content);
        }

        this.nextRowToReturn = rowBuilder.buildRow();
        return true;
    }

    private void openNextFile() {
        final boolean isStreamingDuringRead = StreamingMode.STREAM_DURING_READER_PHASE.equals(this.streamingMode);
        final String nextFilePath = filePartition.getPaths().get(nextFilePathIndex);

        this.currentFilePath = isStreamingDuringRead ? nextFilePath : fileContext.decodeFilePath(nextFilePath);
        nextFilePathIndex++;

        if (!isStreamingDuringRead) {
            this.currentZipInputStream = new ZipInputStream(fileContext.openFile(this.currentFilePath));
        }
    }

    private boolean openNextFileAndReadNextEntry() {
        close();
        if (nextFilePathIndex >= this.filePartition.getPaths().size()) {
            return false;
        }
        openNextFile();
        return next();
    }

    private byte[] readMetadataEntry(String zipEntryName) throws IOException {
        ZipEntry metadataEntry = FileUtil.findNextFileEntry(currentZipInputStream);
        if (metadataEntry == null || !metadataEntry.getName().endsWith(".metadata")) {
            String message = String.format("Could not find metadata entry for entry %s in file %s", zipEntryName, this.currentFilePath);
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message);
            }
            Util.MAIN_LOGGER.warn(message);
            return new byte[0];
        }
        return fileContext.readBytes(currentZipInputStream);
    }

    /**
     * Builds a row to represent the archive file so that it can be opened during the writer phase.
     */
    private InternalRow buildSingleRowForArchiveFile() {
        byte[] serializedFileContext = FileUtil.serializeFileContext(fileContext, currentFilePath);
        InternalRow row = new DocumentRowBuilder(this.metadataCategories)
            .withUri(this.currentFilePath)
            .withContent(serializedFileContext)
            .buildRow();
        this.currentFilePath = null;
        return row;
    }
}
