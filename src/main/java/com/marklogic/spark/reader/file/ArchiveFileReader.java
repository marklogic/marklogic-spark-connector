package com.marklogic.spark.reader.file;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class ArchiveFileReader implements PartitionReader<InternalRow> {

    private final String path;
    private final FileContext fileContext;
    private final ZipInputStream zipInputStream;
    private final List<String> metadataCategories;
    private InternalRow nextRowToReturn;

    ArchiveFileReader(FilePartition partition, FileContext fileContext) {
        this.path = partition.getPath();
        this.fileContext = fileContext;
        this.zipInputStream = new ZipInputStream(fileContext.open(partition));
        this.metadataCategories = new ArrayList<>();
        if (fileContext.hasOption(Options.READ_ARCHIVES_CATEGORIES)) {
            for (String category : fileContext.getStringOption(Options.READ_ARCHIVES_CATEGORIES).split(",")) {
                this.metadataCategories.add(category.toLowerCase());
            }
        }
    }

    @Override
    public boolean next() {
        try {
            ZipEntry contentZipEntry = FileUtil.findNextFileEntry(zipInputStream);
            if (contentZipEntry == null) {
                return false;
            }
            byte[] content = FileUtil.readBytes(zipInputStream);
            if (content == null || content.length == 0) {
                return false;
            }
            final String zipEntryName = contentZipEntry.getName();

            byte[] metadataBytes = readMetadataEntry(zipEntryName);
            if (metadataBytes == null || metadataBytes.length == 0) {
                return false;
            }

            DocumentMetadataHandle metadata = new DocumentMetadataHandle();
            metadata.fromBuffer(metadataBytes);
            this.nextRowToReturn = new DocumentRowBuilder(this.metadataCategories)
                .withUri(zipEntryName).withContent(content).withMetadata(metadata)
                .buildRow();
            return true;
        } catch (IOException e) {
            String message = String.format("Unable to read archive file at %s; cause: %s", this.path, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, e);
            }
            Util.MAIN_LOGGER.warn(message);
            return false;
        }
    }

    @Override
    public InternalRow get() {
        return nextRowToReturn;
    }

    @Override
    public void close() throws IOException {
        this.zipInputStream.close();
    }

    private byte[] readMetadataEntry(String zipEntryName) throws IOException {
        ZipEntry metadataEntry = FileUtil.findNextFileEntry(zipInputStream);
        if (metadataEntry == null || !metadataEntry.getName().endsWith(".metadata")) {
            String message = String.format("Could not find metadata entry for entry %s in file %s", zipEntryName, this.path);
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message);
            }
            Util.MAIN_LOGGER.warn(message);
            return new byte[0];
        }
        return FileUtil.readBytes(zipInputStream);
    }
}
