package com.marklogic.spark.reader.file;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Reduces some duplication across RdfFileReader and QuadsFileReader.
 */
abstract class AbstractRdfFileReader implements PartitionReader<InternalRow> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected InputStream inputStream;

    AbstractRdfFileReader(FilePartition filePartition) {
        if (logger.isDebugEnabled()) {
            logger.debug("Reading RDF file {}", filePartition.getPath());
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.inputStream);
    }

    protected final InputStream openStream(FilePartition filePartition, FileContext fileContext) throws IOException {
        return fileContext.isGzip() ? new GZIPInputStream(fileContext.open(filePartition)) : fileContext.open(filePartition);
    }
}
