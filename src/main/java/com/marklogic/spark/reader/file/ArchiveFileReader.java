package com.marklogic.spark.reader.file;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.reader.document.DocumentRowBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class ArchiveFileReader implements PartitionReader<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ArchiveFileReader.class);
    private final String path;
    private final ZipInputStream zipInputStream;
    private final List<String> metadataCategories;
    private InternalRow nextRowToReturn;

    ArchiveFileReader(FilePartition partition, FileContext fileContext) {
        this.path = partition.getPath();
        this.zipInputStream = new ZipInputStream(fileContext.open(partition));
        this.metadataCategories = new ArrayList<>();
        if (fileContext.hasOption(Options.READ_ARCHIVES_CATEGORIES)) {
            for (String category : fileContext.getStringOption(Options.READ_ARCHIVES_CATEGORIES).split(",")) {
                this.metadataCategories.add(category.toLowerCase());
            }
        }
    }

    @Override
    public boolean next() throws IOException {
        ZipEntry zipEntry = FileUtil.findNextFileEntry(zipInputStream);
        if (zipEntry == null) {
            return false;
        }
        byte[] content = readZipEntry();
        if (content.length == 0) {
            return false;
        }
        String zipEntryName = zipEntry.getName();
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if (logger.isTraceEnabled()) {
            logger.trace("Reading zip entry {} from zip file {}.", zipEntryName, this.path);
        }
        String uri = zipEntryName.startsWith("/") ? this.path + zipEntryName : this.path + "/" + zipEntryName;
        try {
            ZipEntry metadataEntry = FileUtil.findNextFileEntry(zipInputStream);
            if (metadataEntry == null) {
                return false;
            }
            if (!metadataEntry.getName().endsWith(".metadata")) {
                throw new ConnectorException(String.format("Could not find metadata entry for entry %s", zipEntryName));
            }
            metadata.fromBuffer(readZipEntry());
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to read zip file at %s; cause: %s", this.path, e.getMessage()), e);
        }

        this.nextRowToReturn = new DocumentRowBuilder(this.metadataCategories)
            .withUri(uri).withContent(content).withMetadata(metadata).buildRow();
        return true;
    }

    @Override
    public InternalRow get() {
        return nextRowToReturn;
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
