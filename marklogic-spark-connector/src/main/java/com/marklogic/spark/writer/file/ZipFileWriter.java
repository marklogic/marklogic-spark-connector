/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ZipFileWriter.class);

    private final ContextSupport context;
    private final SerializableConfiguration hadoopConfiguration;

    private final String path;
    private final String zipFilePath;

    // These can be instantiated lazily depending on which constructor is used.
    private ContentWriter contentWriter;
    private ZipOutputStream zipOutputStream;

    private int zipEntryCounter;

    ZipFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration, int partitionId) {
        this(properties.get("path"), properties, hadoopConfiguration, partitionId, true);
    }

    public ZipFileWriter(String path, Map<String, String> properties, SerializableConfiguration hadoopConfiguration,
                         int partitionId, boolean createZipFileImmediately) {
        this.path = path;
        this.zipFilePath = makeFilePath(path, partitionId);
        this.context = new ContextSupport(properties);
        this.hadoopConfiguration = hadoopConfiguration;
        if (createZipFileImmediately) {
            createZipFileAndContentWriter();
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        if (contentWriter == null) {
            createZipFileAndContentWriter();
        }
        Objects.requireNonNull(zipOutputStream);
        Objects.requireNonNull(contentWriter);

        final String uri = row.getString(0);
        // As of 2.7.0, not doing any special handling for an "opaque" URI which appears to be fine as a zip entry name.
        final String entryName = uri;

        writeMetadataEntryIfNecessary(row, uri, entryName);
        zipOutputStream.putNextEntry(new ZipEntry(entryName));
        this.contentWriter.writeContent(row, zipOutputStream);
        zipEntryCounter++;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(zipOutputStream);
    }

    @Override
    public WriterCommitMessage commit() {
        return new ZipCommitMessage(path, zipFilePath, zipEntryCounter);
    }

    @Override
    public void abort() {
        // No action to take.
    }

    private void createZipFileAndContentWriter() {
        Path filePath = new Path(zipFilePath);
        if (logger.isDebugEnabled()) {
            logger.debug("Will write to: {}", filePath);
        }
        this.contentWriter = new ContentWriter(context.getProperties());
        try {
            FileSystem fileSystem = filePath.getFileSystem(hadoopConfiguration.value());
            fileSystem.setWriteChecksum(false);
            zipOutputStream = new ZipOutputStream(fileSystem.create(filePath, true));
        } catch (IOException e) {
            throw new ConnectorException("Unable to create stream for writing zip file: " + e.getMessage(), e);
        }
    }

    private void writeMetadataEntryIfNecessary(InternalRow row, String uri, String entryName) throws IOException {
        if (this.context.isStreamingFiles() && context.hasOption(Options.READ_DOCUMENTS_CATEGORIES)) {
            zipOutputStream.putNextEntry(new ZipEntry(entryName + ".metadata"));
            this.contentWriter.writeMetadataWhileStreaming(uri, zipOutputStream);
            zipEntryCounter++;
        } else if (hasMetadata(row)) {
            zipOutputStream.putNextEntry(new ZipEntry(entryName + ".metadata"));
            this.contentWriter.writeMetadata(row, zipOutputStream);
            zipEntryCounter++;
        }
    }

    private boolean hasMetadata(InternalRow row) {
        return !row.isNullAt(3) || !row.isNullAt(4) || !row.isNullAt(5) || !row.isNullAt(6) || !row.isNullAt(7);
    }

    /**
     * Copies some of what MLCP's ArchiveWriter does, but does not create a zip file per document type. The reason
     * for that behavior in MLCP isn't known. It would not help for importing the zip files, where the URI extension will
     * determine the document type. And it seems like unnecessary zip file generation - i.e. if a user wants to export
     * 10k XML and JSON docs in the same collection, getting 2 zip files instead of 1 seems like surprising behavior.
     * Additionally, a user can arrive at that outcome if desired by using Spark to repartion the dataset based on
     * the "format" column.
     *
     * @param path
     * @param partitionId
     * @return
     */
    private String makeFilePath(String path, int partitionId) {
        final String timestamp = new SimpleDateFormat("yyyyMMddHHmmssZ").format(new Date());
        return String.format("%s%s%s-%d.zip", path, File.separator, timestamp, partitionId);
    }

    public String getZipFilePath() {
        return zipFilePath;
    }
}
