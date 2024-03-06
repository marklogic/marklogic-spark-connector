package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Expects to read a single gzipped file and return a single row. May expand the scope of this later to expect multiple
 * files and to thus return multiple rows.
 */
class GZIPFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;
    private boolean fileHasBeenRead;

    GZIPFileReader(FilePartition partition, FileContext fileContext) {
        this.filePartition = partition;
        this.fileContext = fileContext;
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
        InputStream inputStream = fileContext.open(filePartition);
        try {
            return new GZIPInputStream(inputStream);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read gzip file at %s; cause: %s", this.filePartition, e.getMessage()), e);
        }
    }

    private byte[] extractGZIPContents(GZIPInputStream gzipInputStream) {
        try {
            return FileUtil.readBytes(gzipInputStream);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read from gzip file at %s; cause: %s",
                this.filePartition, e.getMessage()), e);
        }
    }

    private String makeURI() {
        String path = filePartition.getPath();
        // Copied from MLCP.
        return path.endsWith(".gzip") || path.endsWith(".gz") ?
            path.substring(0, path.lastIndexOf(".")) :
            path;
    }
}
