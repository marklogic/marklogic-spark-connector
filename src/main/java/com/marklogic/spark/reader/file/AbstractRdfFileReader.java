package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reduces some duplication across RdfFileReader and QuadsFileReader.
 */
abstract class AbstractRdfFileReader implements PartitionReader<InternalRow> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private RdfStreamReader rdfStreamReader;

    private InputStream inputStream;

    AbstractRdfFileReader(FilePartition partition, FileContext fileContext) {
        this.filePartition = partition;
        this.fileContext = fileContext;
    }

    /**
     * Because Jena doesn't have an abstraction over its Triple and Quad classes (seems like a Quad should just extend
     * Triple?), the RdfStreamReader interface is used to abstract that instead. So a subclass must return an instance
     * of what, based on whether it's reading triples or quads.
     *
     * @param path
     * @param inputStream
     * @return
     */
    protected abstract RdfStreamReader initializeRdfStreamReader(String path, InputStream inputStream);

    @Override
    public boolean next() throws IOException {
        if (rdfStreamReader == null && !initializeTripleStreamReader()) {
            return false;
        }

        try {
            return rdfStreamReader.hasNext();
        } catch (RuntimeException ex) {
            if (fileContext.isReadAbortOnFailure()) {
                throw ex;
            }
            Util.MAIN_LOGGER.warn(ex.getMessage());
            return false;
        }
    }

    @Override
    public InternalRow get() {
        return rdfStreamReader.get();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.inputStream);
    }

    private boolean initializeTripleStreamReader() {
        if (logger.isDebugEnabled()) {
            logger.debug("Reading file {}", filePartition.getPath());
        }
        try {
            this.inputStream = fileContext.open(filePartition);
            this.rdfStreamReader = initializeRdfStreamReader(filePartition.getPath(), inputStream);
            return true;
        } catch (Exception e) {
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(String.format(
                    "Unable to read file at %s; cause: %s", filePartition.getPath(), e.getMessage()), e);
            }
            Util.MAIN_LOGGER.warn("Unable to read file at {}; cause: {}", filePartition.getPath(), e.getMessage());
            return false;
        }
    }
}
