/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.crypto.utils.IoUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


class ZipFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ZipFileReader.class);

    private final FilePartition filePartition;
    private final FileContext fileContext;
    private int nextFilePathIndex;
    private String currentFilePath;
    private ZipInputStream currentZipInputStream;
    private ZipEntry currentZipEntry;

    ZipFileReader(FilePartition filePartition, FileContext fileContext) {
        this.filePartition = filePartition;
        this.fileContext = fileContext;
        openNextFile();
    }

    @Override
    public boolean next() throws IOException {
        currentZipEntry = FileUtil.findNextFileEntry(currentZipInputStream);
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
        String zipEntryName = currentZipEntry.getName();
        if (logger.isTraceEnabled()) {
            logger.trace("Reading zip entry {} from zip file {}.", zipEntryName, this.currentFilePath);
        }
        String uri = zipEntryName.startsWith("/") ?
            this.currentFilePath + zipEntryName :
            this.currentFilePath + "/" + zipEntryName;
        byte[] content = readZipEntry();
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri), ByteArray.concat(content),
            null, null, null, null, null, null
        });
    }

    @Override
    public void close() {
        IoUtils.closeQuietly(this.currentZipInputStream);
    }

    private void openNextFile() {
        this.currentFilePath = this.filePartition.getPaths().get(nextFilePathIndex);
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
}
