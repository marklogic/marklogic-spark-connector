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
        currentZipEntry = findNextValidEntry();
        return currentZipEntry != null;
    }

    @Override
    public InternalRow get() {
        String zipEntryName = currentZipEntry.getName();
        if (logger.isDebugEnabled()) {
            logger.debug("Reading zip entry {} from zip file {}.", zipEntryName, this.path);
        }
        String uri = this.path + "/" + zipEntryName;
        byte[] content = readZipEntry();
        Object modificationTime = null; // We don't yet have a need to populate this, nor a way to determine it.
        long length = content.length;
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri), modificationTime, length, ByteArray.concat(content)
        });
    }

    @Override
    public void close() throws IOException {
        this.zipInputStream.close();
    }

    // The suppressed Sonar warning is for a potential "Zip bomb" attack when accessing a zip entry. The recommended
    // fixes involve checking the zip of the entry. That is not possible as the ZipInputStream returns -1 for both the
    // size and compressed size.
    @SuppressWarnings("java:S5042")
    private ZipEntry findNextValidEntry() throws IOException {
        ZipEntry entry = zipInputStream.getNextEntry();
        if (entry == null) {
            return null;
        }
        return !entry.isDirectory() ? entry : findNextValidEntry();
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
