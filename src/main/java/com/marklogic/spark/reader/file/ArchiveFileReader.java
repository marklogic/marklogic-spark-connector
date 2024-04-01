package com.marklogic.spark.reader.file;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.reader.document.DocumentRowSchema;
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

class ArchiveFileReader implements PartitionReader<InternalRow> {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveFileReader.class);
    private final String path;
    private final ZipInputStream zipInputStream;
    private ZipEntry currentZipEntry;

    public ArchiveFileReader(FilePartition partition, FileContext fileContext) {
        this.path = partition.getPath();
        this.zipInputStream = new ZipInputStream(fileContext.open(partition));
    }

    @Override
    public boolean next() throws IOException {
        currentZipEntry = FileUtil.findNextFileEntry(zipInputStream);
        return currentZipEntry != null;
    }

    @Override
    public InternalRow get() {

        byte[] content = readZipEntry();
        String zipEntryName = currentZipEntry.getName();
        if (logger.isTraceEnabled()) {
            logger.trace("Reading zip entry {} from zip file {}.", zipEntryName, this.path);
        }
        String uri = zipEntryName.startsWith("/") ? this.path + zipEntryName : this.path + "/" + zipEntryName;
        try {
            ZipEntry metadataEntry = FileUtil.findNextFileEntry(zipInputStream);
            if (metadataEntry == null || !metadataEntry.getName().endsWith(".metadata")) {
                throw new ConnectorException(String.format("Could not find metadata entry for entry %s", zipEntryName));
            }
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read zip file at %s; cause: %s", this.path, e.getMessage()), e);
        }

        DocumentMetadataHandle documentMetadataHandle = new DocumentMetadataHandle();
        documentMetadataHandle.fromBuffer(readZipEntry());

        return makeRow(uri, content, documentMetadataHandle);
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

    private InternalRow makeRow(String uri, byte[] content, DocumentMetadataHandle documentMetadataHandle) {

        Object[] row = new Object[8];
        row[0] = UTF8String.fromString(uri);
        row[1] = ByteArray.concat(content);
        if (documentMetadataHandle.getFormat() != null) {
            row[2] = UTF8String.fromString(documentMetadataHandle.getFormat().name());
        }
        DocumentRowSchema.populateAllMetadata(row, documentMetadataHandle);
        return new GenericInternalRow(row);
    }
}
