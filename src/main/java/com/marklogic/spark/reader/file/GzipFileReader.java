package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Util;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.InputStream;

/**
 * Expects to read a single gzipped file and return a single row. May expand the scope of this later to expect multiple
 * files and to thus return multiple rows.
 */
class GzipFileReader implements PartitionReader<InternalRow> {

    private final FilePartition filePartition;
    private final FileContext fileContext;

    private InternalRow rowToReturn = null;

    GzipFileReader(FilePartition partition, FileContext fileContext) {
        this.filePartition = partition;
        this.fileContext = fileContext;
    }

    @Override
    public boolean next() {
        if (rowToReturn != null) {
            return false;
        }

        // Must capture the row here so that if an error occurs and it should not cause a failure, it can be logged here
        // and false can be returned.
        InputStream gzipInputStream = null;
        try {
            gzipInputStream = fileContext.open(filePartition);
            byte[] content = extractGZIPContents(gzipInputStream);
            String uri = makeURI();
            long length = content.length;
            this.rowToReturn = new GenericInternalRow(new Object[]{
                UTF8String.fromString(uri), null, length, ByteArray.concat(content)
            });
            return true;
        } catch (RuntimeException ex) {
            if (fileContext.isReadAbortOnFailure()) {
                throw ex;
            }
            Util.MAIN_LOGGER.warn("Unable to read file at {}; cause: {}", this.filePartition.getPath(), ex.getMessage());
            return false;
        } finally {
            IOUtils.closeQuietly(gzipInputStream);
        }
    }

    @Override
    public InternalRow get() {
        return rowToReturn;
    }

    @Override
    public void close() {
        // Nothing to close.
    }

    private byte[] extractGZIPContents(InputStream gzipInputStream) {
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
