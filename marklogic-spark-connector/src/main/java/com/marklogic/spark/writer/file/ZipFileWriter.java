/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileWriter implements DataWriter<InternalRow> {

    // Default threshold for logging a warning about large zip files. Based on some math with Copilot about heap usage:
    // 46 bytes per zip entry; 100 bytes on average for URI length; 500k entries = ~70MB of heap usage.
    // With a default of 10 Spark tasks per executor, that would be ~700MB of heap usage if all tasks hit the threshold.
    // That is a reasonable time to log a warning about potential heap space usage.
    private static final int DEFAULT_WARN_THRESHOLD = 500000;

    private final ContextSupport context;
    private final SerializableConfiguration hadoopConfiguration;

    private final String path;
    private final Path zipPath;
    private final int warnThreshold;

    // These can be instantiated lazily depending on which constructor is used.
    private ContentWriter contentWriter;
    private ZipOutputStream zipOutputStream;

    private int zipEntryCounter;
    private boolean hasLoggedThresholdWarning;

    ZipFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration, int partitionId) {
        this(properties.get("path"), properties, hadoopConfiguration, partitionId, true);
    }

    public ZipFileWriter(String path, Map<String, String> properties, SerializableConfiguration hadoopConfiguration,
                         int partitionId, boolean createZipFileImmediately) {
        this.path = path;
        this.zipPath = makeFilePath(path, partitionId);
        this.context = new ContextSupport(properties);
        this.hadoopConfiguration = hadoopConfiguration;
        this.warnThreshold = context.getIntOption(Options.WRITE_FILES_ZIP_WARN_THRESHOLD, DEFAULT_WARN_THRESHOLD, 0);

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

        String format = null;
        InputStreamHandle streamingContentHandle = null;
        if (contentWriter.isStreamingFiles()) {
            // When streaming, we need to open the content handle before writing the metadata entry so that the
            // document format can be obtained and used in the metadata entry name.
            streamingContentHandle = contentWriter.readContent(uri);
            format = streamingContentHandle.getFormat() != null ? streamingContentHandle.getFormat().toString() : null;
        }
        writeMetadataEntryIfNecessary(row, uri, entryName, format);
        zipOutputStream.putNextEntry(new ZipEntry(entryName));
        this.contentWriter.writeContent(row, zipOutputStream, streamingContentHandle);
        zipEntryCounter++;
        logThresholdWarningIfNecessary();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(zipOutputStream);
        IOUtils.closeQuietly(contentWriter);
    }

    @Override
    public WriterCommitMessage commit() {
        return new ZipCommitMessage(path, zipPath.toString(), zipEntryCounter);
    }

    @Override
    public void abort() {
        // No action to take.
    }

    private void createZipFileAndContentWriter() {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Will write file at: {}", zipPath);
        }
        this.contentWriter = new ContentWriter(context.getProperties());
        try {
            FileSystem fileSystem = zipPath.getFileSystem(hadoopConfiguration.value());
            fileSystem.setWriteChecksum(false);
            zipOutputStream = new ZipOutputStream(fileSystem.create(zipPath, true));
        } catch (IOException e) {
            throw new ConnectorException("Unable to create stream for writing zip file: " + e.getMessage(), e);
        }
    }

    private void writeMetadataEntryIfNecessary(InternalRow row, String uri, String entryName, String format) throws IOException {
        if (format == null) {
            format = DocumentRowSchema.getFormat(row);
        }
        if (this.context.isStreamingFiles() && context.hasOption(Options.READ_DOCUMENTS_CATEGORIES)) {
            final String metadataEntryName = buildMetadataEntryName(entryName, format);
            zipOutputStream.putNextEntry(new ZipEntry(metadataEntryName));
            this.contentWriter.writeMetadataWhileStreaming(uri, zipOutputStream);
            zipEntryCounter++;
        } else if (hasMetadata(row)) {
            final String metadataEntryName = buildMetadataEntryName(entryName, format);
            zipOutputStream.putNextEntry(new ZipEntry(metadataEntryName));
            this.contentWriter.writeMetadata(row, zipOutputStream);
            zipEntryCounter++;
        }
    }

    /**
     * Builds a metadata entry name encoding the document format.
     * This allows import logic to detect the format during import.
     *
     * @param entryName the document URI/entry name
     * @param format    the document format, which can be null
     * @return the metadata entry name (e.g., "/doc.xml.BINARY.metadata")
     */
    private String buildMetadataEntryName(String entryName, String format) {
        MetadataEntryName metadataEntryName = new MetadataEntryName(entryName, format);
        return metadataEntryName.makeMetadataEntryName();
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
    private Path makeFilePath(String path, int partitionId) {
        final String timestamp = new SimpleDateFormat("yyyyMMddHHmmssZ").format(new Date());
        final String zipFilename = String.format("%s-%d.zip", timestamp, partitionId);
        // Fixed a bug in 1.x (fixed in 2.0) by using a Path here instead of string concatenation with File.separator.
        // Using File.separator on Windows would result in a "\" in an S3 URL, which is awkward for a user to work with,
        // and appears buggy and unexpected.
        return new Path(path, zipFilename);
    }

    private void logThresholdWarningIfNecessary() {
        if (warnThreshold > 0 && !hasLoggedThresholdWarning && zipEntryCounter >= warnThreshold) {
            Util.MAIN_LOGGER.warn(
                "Zip file {} has {} entries. Large zip files keep entry metadata in memory, which may cause JVM heap pressure. " +
                    "To reduce entries per file, increase the number of partitions per forest for reading data from MarkLogic.",
                zipPath.getName(), zipEntryCounter
            );
            hasLoggedThresholdWarning = true;
        }
    }

    public String getZipFilePath() {
        return zipPath.toString();
    }
}
