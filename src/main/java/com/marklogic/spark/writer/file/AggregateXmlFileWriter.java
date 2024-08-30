/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
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
 * For now, forcing the usage of pretty-printing. That ensures that XML declarations don't appear.
 * <p>
 * TODO Could support gzip here easily as well.
 */
class AggregateXmlFileWriter implements DataWriter<InternalRow> {

    private final Map<String, String> properties;
    private final SerializableConfiguration hadoopConfiguration;
    private final String xmlFilePath;
    private final String rootElement;
    private final String rootNamespace;

    // Created lazily, in case this writer never receives any rows.
    private BufferedOutputStream xmlOutputStream;
    private ContentWriter contentWriter;

    private int elementCount;

    AggregateXmlFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration, int partitionId) {
        this.properties = properties;
        this.xmlFilePath = makeFilePath(properties.get("path"), partitionId);
        this.rootElement = properties.get(Options.WRITE_AGGREGATES_XML_ELEMENT);
        this.rootNamespace = properties.get(Options.WRITE_AGGREGATES_XML_NAMESPACE);
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        if (xmlOutputStream == null) {
            initializeOutputStream();
        }
        this.contentWriter.writeContent(row, xmlOutputStream);
        elementCount++;
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        if (xmlOutputStream != null) {
            String closingTag = String.format("</%s>", rootElement);
            xmlOutputStream.write(closingTag.getBytes());
            return new AggregateXmlCommitMessage(xmlFilePath, elementCount);
        }
        return null;
    }

    @Override
    public void abort() {
        // No action to take.
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(xmlOutputStream);
    }

    private String makeFilePath(String path, int partitionId) {
        final String timestamp = new SimpleDateFormat("yyyyMMddHHmmssZ").format(new Date());
        return String.format("%s%s%s-%d.xml", path, File.separator, timestamp, partitionId);
    }

    private void initializeOutputStream() {
        Path hadoopPath = new Path(xmlFilePath);
        try {
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            fileSystem.setWriteChecksum(false);
            xmlOutputStream = new BufferedOutputStream(fileSystem.create(hadoopPath, true));
            String rootTag = this.rootNamespace != null ?
                String.format("<%s xmlns='%s'>\n", rootElement, rootNamespace) :
                String.format("<%s>\n", rootElement);
            xmlOutputStream.write(rootTag.getBytes());
            this.contentWriter = new ContentWriter(properties, true);
        } catch (IOException e) {
            throw new ConnectorException("Unable to create stream for writing XML file: " + e.getMessage(), e);
        }
    }

    static class AggregateXmlCommitMessage implements WriterCommitMessage {
        private final String path;
        private final int elementCount;

        public AggregateXmlCommitMessage(String path, int elementCount) {
            this.path = path;
            this.elementCount = elementCount;
        }

        public String getPath() {
            return path;
        }

        public int getElementCount() {
            return elementCount;
        }
    }
}
