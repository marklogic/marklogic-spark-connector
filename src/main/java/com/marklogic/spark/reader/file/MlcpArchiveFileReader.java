package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class MlcpArchiveFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(MlcpArchiveFileReader.class);

    private final String path;
    private final ZipInputStream zipInputStream;
    private ZipEntry currentZipEntry;

    MlcpArchiveFileReader(FilePartition filePartition, FileContext fileContext) {
        this.path = filePartition.getPath();
        this.zipInputStream = new ZipInputStream(fileContext.open(filePartition));
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
        byte[] content = new byte[0];
        try {
            content = FileUtil.readBytes(zipInputStream);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read from zip file at %s; cause: %s",
                this.path, e.getMessage()), e);
        }
        long length = content.length;
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri), null, length, ByteArray.concat(content)
        });
    }

    @Override
    public void close() throws IOException {
        this.zipInputStream.close();
    }
}
