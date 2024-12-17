/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class MlcpArchiveFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final MlcpMetadataConverter mlcpMetadataConverter;
    private final FileContext fileContext;
    private final List<String> metadataCategories;

    private String currentFilePath;
    private ZipInputStream currentZipInputStream;
    private int nextFilePathIndex;
    private InternalRow nextRowToReturn;

    MlcpArchiveFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
        this.mlcpMetadataConverter = new MlcpMetadataConverter();

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
        ZipEntry metadataZipEntry = getNextMetadataEntry();
        if (metadataZipEntry == null) {
            return openNextFileAndReadNextEntry();
        }

        MlcpMetadata mlcpMetadata = readMetadataEntry(metadataZipEntry);
        if (mlcpMetadata == null) {
            return openNextFileAndReadNextEntry();
        }

        if (metadataZipEntry.getName().endsWith(".naked")) {
            if (readNakedEntry(metadataZipEntry, mlcpMetadata)) {
                return true;
            }
            return openNextFileAndReadNextEntry();
        }

        ZipEntry contentZipEntry = getContentEntry(metadataZipEntry);
        if (contentZipEntry == null) {
            return openNextFileAndReadNextEntry();
        }

        byte[] content = readBytesFromContentEntry(contentZipEntry);
        if (content == null || content.length == 0) {
            return openNextFileAndReadNextEntry();
        }

        try {
            nextRowToReturn = makeRow(contentZipEntry, content, mlcpMetadata);
            return true;
        } catch (Exception ex) {
            String message = String.format("Unable to process entry %s from zip file at %s; cause: %s",
                contentZipEntry.getName(), this.currentFilePath, ex.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, ex);
            }
            Util.MAIN_LOGGER.warn(message);
            return openNextFileAndReadNextEntry();
        }
    }

    @Override
    public InternalRow get() {
        return nextRowToReturn;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(this.currentZipInputStream);
    }

    private void openNextFile() {
        this.currentFilePath = filePartition.getPaths().get(nextFilePathIndex);
        nextFilePathIndex++;
        this.currentZipInputStream = new ZipInputStream(fileContext.openFile(this.currentFilePath));
    }

    private boolean openNextFileAndReadNextEntry() {
        close();
        if (nextFilePathIndex >= this.filePartition.getPaths().size()) {
            return false;
        }
        openNextFile();
        return next();
    }

    private ZipEntry getNextMetadataEntry() {
        // MLCP always includes a metadata entry, even if the user asks for no metadata. And the metadata entry is
        // always first.
        try {
            return FileUtil.findNextFileEntry(currentZipInputStream);
        } catch (IOException e) {
            String message = String.format("Unable to read from zip file: %s; cause: %s", this.currentFilePath, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, e);
            }
            Util.MAIN_LOGGER.warn(message);
            return null;
        }
    }

    private MlcpMetadata readMetadataEntry(ZipEntry metadataZipEntry) {
        try {
            return this.mlcpMetadataConverter.convert(new ByteArrayInputStream(fileContext.readBytes(currentZipInputStream)));
        } catch (Exception e) {
            String message = String.format("Unable to read metadata for entry: %s; file: %s; cause: %s",
                metadataZipEntry.getName(), this.currentFilePath, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, e);
            }
            // Contrary to a zip of aggregate XML files, if we get a bad metadata file, we likely do not have a valid
            // MLCP archive file and thus there's no reason to continue processing this particular file.
            Util.MAIN_LOGGER.warn(message);
            return null;
        }
    }

    private ZipEntry getContentEntry(ZipEntry metadataZipEntry) {
        ZipEntry contentZipEntry;
        try {
            contentZipEntry = FileUtil.findNextFileEntry(currentZipInputStream);
        } catch (IOException e) {
            String message = String.format("Unable to read content entry from file: %s; cause: %s", this.currentFilePath, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, e);
            }
            Util.MAIN_LOGGER.warn(message);
            return null;
        }

        if (contentZipEntry == null) {
            String message = String.format("No content entry found for metadata entry: %s; file: %s", metadataZipEntry.getName(), this.currentFilePath);
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message);
            }
            Util.MAIN_LOGGER.warn(message);
            return null;
        }
        return contentZipEntry;
    }

    private byte[] readBytesFromContentEntry(ZipEntry contentZipEntry) {
        try {
            return fileContext.readBytes(currentZipInputStream);
        } catch (IOException e) {
            String message = String.format("Unable to read entry %s from zip file at %s; cause: %s",
                contentZipEntry.getName(), this.currentFilePath, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, e);
            }
            Util.MAIN_LOGGER.warn(message);
            return new byte[0];
        }
    }

    /**
     * A "naked" entry refers to a properties fragment with no associate document. MLCP supports exporting these, and
     * so we need to support reading them in from an MLCP archive file.
     */
    private boolean readNakedEntry(ZipEntry metadataZipEntry, MlcpMetadata mlcpMetadata) {
        try {
            nextRowToReturn = makeNakedRow(metadataZipEntry, mlcpMetadata);
            return true;
        } catch (Exception ex) {
            String message = String.format("Unable to process entry %s from zip file at %s; cause: %s",
                metadataZipEntry.getName(), this.currentFilePath, ex.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, ex);
            }
            Util.MAIN_LOGGER.warn(message);
            return false;
        }
    }

    private InternalRow makeNakedRow(ZipEntry metadataZipEntry, MlcpMetadata mlcpMetadata) {
        return new DocumentRowBuilder(metadataCategories)
            .withUri(metadataZipEntry.getName())
            .withMetadata(mlcpMetadata.getMetadata())
            .buildRow();
    }

    private InternalRow makeRow(ZipEntry contentZipEntry, byte[] content, MlcpMetadata mlcpMetadata) {
        DocumentRowBuilder rowBuilder = new DocumentRowBuilder(metadataCategories)
            .withUri(contentZipEntry.getName())
            .withContent(content)
            .withMetadata(mlcpMetadata.getMetadata());

        if (mlcpMetadata.getFormat() != null) {
            rowBuilder.withFormat(mlcpMetadata.getFormat().name());
        }
        return rowBuilder.buildRow();
    }
}
