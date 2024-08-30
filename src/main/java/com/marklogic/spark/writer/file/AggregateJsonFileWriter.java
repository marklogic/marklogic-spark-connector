/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Aggregate = a JSON array, where each document is an object in the array.
 */
class AggregateJsonFileWriter implements DataWriter<InternalRow> {

    private final Map<String, String> properties;
    private final SerializableConfiguration hadoopConfiguration;
    private final String jsonFilePath;

    // Created lazily, in case this writer never receives any rows.
    private BufferedOutputStream jsonOutputStream;
    private ContentWriter contentWriter;

    private int documentCount;

    AggregateJsonFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration, int partitionId) {
        this.properties = properties;
        this.jsonFilePath = makeFilePath(properties.get("path"), partitionId);
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        if (jsonOutputStream == null) {
            initializeOutputStream();
        }
        if (documentCount > 0) {
            jsonOutputStream.write(",\n".getBytes());
        }
        this.contentWriter.writeContent(row, jsonOutputStream);
        documentCount++;
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        if (jsonOutputStream != null) {
            jsonOutputStream.write("\n]".getBytes());
            return new AggregateJsonCommitMessage(jsonFilePath, documentCount);
        }
        return null;
    }

    @Override
    public void abort() {
        // No action to take.
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(jsonOutputStream);
    }

    private String makeFilePath(String path, int partitionId) {
        final String timestamp = new SimpleDateFormat("yyyyMMddHHmmssZ").format(new Date());
        return String.format("%s%s%s-%d.json", path, File.separator, timestamp, partitionId);
    }

    private void initializeOutputStream() {
        Path hadoopPath = new Path(jsonFilePath);
        try {
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            fileSystem.setWriteChecksum(false);
            jsonOutputStream = new BufferedOutputStream(fileSystem.create(hadoopPath, true));
            jsonOutputStream.write("[\n".getBytes());
            this.contentWriter = new ContentWriter(properties);
        } catch (IOException e) {
            throw new ConnectorException("Unable to create stream for writing XML file: " + e.getMessage(), e);
        }
    }

    static class AggregateJsonCommitMessage implements WriterCommitMessage {
        private final String path;
        private final int documentCount;

        public AggregateJsonCommitMessage(String path, int documentCount) {
            this.path = path;
            this.documentCount = documentCount;
        }

        public String getPath() {
            return path;
        }

        public int getDocumentCount() {
            return documentCount;
        }
    }
}

