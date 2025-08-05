/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

/**
 * Writes each row, which is expected to represent a document, as a single file.
 */
class DocumentFileWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(DocumentFileWriter.class);

    private final SerializableConfiguration hadoopConfiguration;
    private final ContentWriter contentWriter;

    private final String path;
    private int fileCounter;

    DocumentFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        this.path = properties.get("path");
        this.hadoopConfiguration = hadoopConfiguration;
        this.contentWriter = new ContentWriter(properties);
    }

    @Override
    public void write(InternalRow row) throws IOException {
        final Path filePath = makeFilePath(row);
        if (logger.isTraceEnabled()) {
            logger.trace("Will write to: {}", filePath);
        }
        OutputStream outputStream = makeOutputStream(filePath);
        try {
            this.contentWriter.writeContent(row, outputStream);
            fileCounter++;
        } finally {
            IOUtils.closeQuietly(outputStream);
        }
    }

    @Override
    public WriterCommitMessage commit() {
        return new FileCommitMessage(this.path, fileCounter);
    }

    @Override
    public void abort() {
        // No action to take.
    }

    @Override
    public void close() {
        // Nothing to close.
    }

    private Path makeFilePath(InternalRow row) {
        final String uri = row.getString(0);
        String filePath = makeFilePath(uri);
        return filePath.charAt(0) == '/' ? new Path(this.path + filePath) : new Path(this.path, filePath);
    }

    // Protected so it can be overridden by subclass.
    protected String makeFilePath(String uri) {
        return FileUtil.makePathFromDocumentURI(uri);
    }

    // Protected so it can be overridden by subclass.
    protected OutputStream makeOutputStream(Path path) throws IOException {
        FileSystem fileSystem = path.getFileSystem(this.hadoopConfiguration.value());
        // MLCP doesn't write .crc files, not sure yet if we want to default this to true or false.
        fileSystem.setWriteChecksum(false);
        // Copied from MLCP; testing shows an enormous improvement in performance when writing to local files by
        // using this approach.
        if (fileSystem instanceof LocalFileSystem) {
            File file = new File(path.toUri().getPath());
            if (!file.exists() && file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            return new BufferedOutputStream(new FileOutputStream(file, false));
        }
        return new BufferedOutputStream(fileSystem.create(path, false));
    }
}
