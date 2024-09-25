/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.client.io.DocumentMetadataHandle;
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

class ArchiveFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;
    private final List<String> metadataCategories;

    private String currentFilePath;
    private ZipInputStream currentZipInputStream;
    private int nextFilePathIndex;
    private InternalRow nextRowToReturn;

    ArchiveFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
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
        try {
            ZipEntry contentZipEntry = FileUtil.findNextFileEntry(currentZipInputStream);
            if (contentZipEntry == null) {
                return openNextFileAndReadNextEntry();
            }
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
        return nextRowToReturn;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(this.currentZipInputStream);
    }

    private void openNextFile() {
        this.currentFilePath = fileContext.getDecodedFilePath(filePartition, nextFilePathIndex);
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
}
