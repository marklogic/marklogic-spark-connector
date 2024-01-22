package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


class ZipFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ZipFileReader.class);

    private final String path;
    private final ZipInputStream zipInputStream;
    private ZipEntry currentZipEntry;

    ZipFileReader(FilePartition partition, SerializableConfiguration hadoopConfiguration) {
        this.path = partition.getPath();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Reading zip file {}", this.path);
            }
            Path hadoopPath = new Path(this.path);
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            this.zipInputStream = new ZipInputStream(fileSystem.open(hadoopPath));
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read zip file at %s; cause: %s", this.path, e.getMessage()), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        currentZipEntry = FileUtil.findNextFileEntry(zipInputStream);
        return currentZipEntry != null;
    }

    @Override
    public InternalRow get() {
        String zipEntryName = currentZipEntry.getName();
        if (logger.isTraceEnabled()) {
            logger.trace("Reading zip entry {} from zip file {}.", zipEntryName, this.path);
        }
        String uri = zipEntryName.startsWith("/") ?
            this.path + zipEntryName :
            this.path + "/" + zipEntryName;
        byte[] content = readZipEntry();
        long length = content.length;
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri), null, length, ByteArray.concat(content)
        });
    }

    @Override
    public void close() throws IOException {
        this.zipInputStream.close();
    }

    private byte[] readZipEntry() {
        try {
            return FileUtil.readBytes(zipInputStream);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read from zip file at %s; cause: %s",
                this.path, e.getMessage()), e);
        }
    }
}
