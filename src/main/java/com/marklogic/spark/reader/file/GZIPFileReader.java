package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Expects to read a single gzipped file and return a single row. May expand the scope of this later to expect multiple
 * files and to thus return multiple rows.
 */
class GZIPFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(GZIPFileReader.class);

    private final String path;
    private final SerializableConfiguration hadoopConfiguration;
    private boolean fileHasBeenRead;

    GZIPFileReader(FilePartition partition, SerializableConfiguration hadoopConfiguration) {
        this.path = partition.getPath();
        this.hadoopConfiguration = hadoopConfiguration;

    }

    @Override
    public boolean next() {
        return !fileHasBeenRead;
    }

    @Override
    public InternalRow get() {
        GZIPInputStream gzipInputStream = openGZIPFile();
        byte[] content = extractGZIPContents(gzipInputStream);
        IOUtils.closeQuietly(gzipInputStream);
        this.fileHasBeenRead = true;

        String uri = makeURI();
        long length = content.length;
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri), null, length, ByteArray.concat(content)
        });
    }

    @Override
    public void close() {
        // Nothing to close.
    }

    private GZIPInputStream openGZIPFile() {
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Reading gzip file {}", this.path);
            }
            Path hadoopPath = new Path(this.path);
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            return new GZIPInputStream(fileSystem.open(hadoopPath));
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read gzip file at %s; cause: %s", this.path, e.getMessage()), e);
        }
    }

    private byte[] extractGZIPContents(GZIPInputStream gzipInputStream) {
        try {
            return FileUtil.readBytes(gzipInputStream);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read from gzip file at %s; cause: %s",
                this.path, e.getMessage()), e);
        }
    }

    private String makeURI() {
        // Copied from MLCP.
        return path.endsWith(".gzip") || path.endsWith(".gz") ?
            path.substring(0, path.lastIndexOf(".")) :
            path;
    }
}
