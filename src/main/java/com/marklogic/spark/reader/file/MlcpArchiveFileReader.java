package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.document.DocumentRowBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class MlcpArchiveFileReader implements PartitionReader<InternalRow> {

    private final String path;
    private final ZipInputStream zipInputStream;
    private final MlcpMetadataConverter mlcpMetadataConverter;
    private final FileContext fileContext;
    private final List<String> metadataCategories;
    private InternalRow nextRowToReturn;

    MlcpArchiveFileReader(FilePartition filePartition, FileContext fileContext) {
        this.path = filePartition.getPath();
        this.fileContext = fileContext;
        this.zipInputStream = new ZipInputStream(fileContext.open(filePartition));
        this.mlcpMetadataConverter = new MlcpMetadataConverter();

        this.metadataCategories = new ArrayList<>();
        if (fileContext.hasOption(Options.READ_ARCHIVES_CATEGORIES)) {
            for (String category : fileContext.getStringOption(Options.READ_ARCHIVES_CATEGORIES).split(",")) {
                this.metadataCategories.add(category.toLowerCase());
            }
        }
    }

    @Override
    public boolean next() {
        ZipEntry metadataZipEntry = getNextMetadataEntry();
        if (metadataZipEntry == null) {
            return false;
        }

        MlcpMetadata mlcpMetadata = readMetadataEntry(metadataZipEntry);
        if (mlcpMetadata == null) {
            return false;
        }

        ZipEntry contentZipEntry = getContentEntry(metadataZipEntry);
        if (contentZipEntry == null) {
            return false;
        }

        byte[] content = readBytesFromContentEntry(contentZipEntry);
        if (content == null || content.length == 0) {
            return false;
        }

        try {
            nextRowToReturn = makeRow(contentZipEntry, content, mlcpMetadata);
            return true;
        } catch (Exception ex) {
            String message = String.format("Unable to process entry %s from zip file at %s; cause: %s",
                contentZipEntry.getName(), this.path, ex.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message);
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
    public void close() {
        IOUtils.closeQuietly(this.zipInputStream);
    }

    private ZipEntry getNextMetadataEntry() {
        // MLCP always includes a metadata entry, even if the user asks for no metadata. And the metadata entry is
        // always first.
        try {
            return FileUtil.findNextFileEntry(zipInputStream);
        } catch (IOException e) {
            String message = String.format("Unable to read from zip file: %s; cause: %s", this.path, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, e);
            }
            Util.MAIN_LOGGER.warn(message);
            return null;
        }
    }

    private MlcpMetadata readMetadataEntry(ZipEntry metadataZipEntry) {
        try {
            return this.mlcpMetadataConverter.convert(new ByteArrayInputStream(FileUtil.readBytes(zipInputStream)));
        } catch (Exception e) {
            String message = String.format("Unable to read metadata for entry: %s; file: %s; cause: %s",
                metadataZipEntry.getName(), this.path, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, e);
            }
            // Contrary to a zip of aggregate XML files, if we get a bad metadata file, we likely do not have a valid
            // MLCP archive file and thus there's no reason to continue processing this particular file.
            Util.MAIN_LOGGER.warn(message);
            return null;
        }
    }

    private ZipEntry getContentEntry(ZipEntry metadataZipEntry) {
        ZipEntry contentZipEntry;
        try {
            contentZipEntry = FileUtil.findNextFileEntry(zipInputStream);
        } catch (IOException e) {
            String message = String.format("Unable to read content entry from file: %s; cause: %s", this.path, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message, e);
            }
            Util.MAIN_LOGGER.warn(message);
            return null;
        }

        if (contentZipEntry == null) {
            String message = String.format("No content entry found for metadata entry: %s; file: %s", metadataZipEntry.getName(), this.path);
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message);
            }
            Util.MAIN_LOGGER.warn(message);
            return null;
        }
        return contentZipEntry;
    }

    private byte[] readBytesFromContentEntry(ZipEntry contentZipEntry) {
        try {
            return FileUtil.readBytes(zipInputStream);
        } catch (IOException e) {
            String message = String.format("Unable to read entry %s from zip file at %s; cause: %s",
                contentZipEntry.getName(), this.path, e.getMessage());
            if (fileContext.isReadAbortOnFailure()) {
                throw new ConnectorException(message);
            }
            Util.MAIN_LOGGER.warn(message);
            return new byte[0];
        }
    }

    private InternalRow makeRow(ZipEntry contentZipEntry, byte[] content, MlcpMetadata mlcpMetadata) {
        final String zipEntryName = contentZipEntry.getName();
        final String uri = zipEntryName.startsWith("/") ?
            this.path + zipEntryName :
            this.path + "/" + zipEntryName;

        DocumentRowBuilder rowBuilder = new DocumentRowBuilder(metadataCategories)
            .withUri(uri).withContent(content).withMetadata(mlcpMetadata.getMetadata());

        if (mlcpMetadata.getFormat() != null) {
            rowBuilder.withFormat(mlcpMetadata.getFormat().name());
        }
        return rowBuilder.buildRow();
    }
}
