package com.marklogic.spark.reader.file;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class ZipAggregateXMLFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ZipAggregateXMLFileReader.class);

    private final FileContext fileContext;
    private final ZipInputStream zipInputStream;
    private final String path;

    private AggregateXMLSplitter aggregateXMLSplitter;

    // Used solely for a default URI prefix.
    private int entryCounter;

    ZipAggregateXMLFileReader(FilePartition filePartition, FileContext fileContext) {
        this.fileContext = fileContext;
        this.path = filePartition.getPath();
        this.zipInputStream = new ZipInputStream(fileContext.open(filePartition));
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
        aggregateXMLSplitter = new AggregateXMLSplitter(identifierForError, this.zipInputStream, this.fileContext.getProperties());
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
