package com.marklogic.spark.reader.file;

import com.marklogic.spark.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
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

    protected final InputStream openStream(Path path, SerializableConfiguration hadoopConfiguration, Map<String, String> properties) throws IOException {
        final boolean isGzipped = "gzip".equalsIgnoreCase(properties.get(Options.READ_FILES_COMPRESSION));
        return isGzipped ?
            new GZIPInputStream(path.getFileSystem(hadoopConfiguration.value()).open(path)) :
            path.getFileSystem(hadoopConfiguration.value()).open(path);
    }
}
