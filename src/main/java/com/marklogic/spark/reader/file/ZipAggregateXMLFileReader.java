package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class ZipAggregateXMLFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ZipAggregateXMLFileReader.class);

    private final Map<String, String> properties;
    private final ZipInputStream zipInputStream;
    private final String path;

    private AggregateXMLSplitter aggregateXMLSplitter;

    // Used solely for a default URI prefix.
    private int entryCounter;

    ZipAggregateXMLFileReader(FilePartition partition, Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        this.properties = properties;
        this.path = partition.getPath();
        if (logger.isTraceEnabled()) {
            logger.trace("Reading path: {}", this.path);
        }
        try {
            Path hadoopPath = new Path(partition.getPath());
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            this.zipInputStream = new ZipInputStream(fileSystem.open(hadoopPath));
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read %s; cause: %s", this.path, e.getMessage()), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        if (aggregateXMLSplitter != null && aggregateXMLSplitter.hasNext()) {
            return true;
        }

        // Once we no longer have any valid zip entries, we're done.
        ZipEntry zipEntry = FileUtil.findNextFileEntry(zipInputStream);
        if (zipEntry == null) {
            return false;
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Reading entry {} in {}", zipEntry.getName(), this.path);
        }
        entryCounter++;
        String identifierForError = "entry " + zipEntry.getName() + " in " + this.path;
        aggregateXMLSplitter = new AggregateXMLSplitter(identifierForError, this.zipInputStream, properties);
        return true;
    }

    @Override
    public InternalRow get() {
        return this.aggregateXMLSplitter.nextRow(this.path + "-" + entryCounter);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.zipInputStream);
    }
}
