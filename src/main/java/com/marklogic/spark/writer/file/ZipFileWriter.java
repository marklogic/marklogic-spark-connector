package com.marklogic.spark.writer.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

class ZipFileWriter implements DataWriter<InternalRow> {

    private static final Logger logger = LoggerFactory.getLogger(ZipFileWriter.class);

    private final ContentWriter contentWriter;
    private final String path;

    private ZipOutputStream zipOutputStream;

    private int zipEntryCounter;

    private Map<String, String> properties;

    ZipFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration, int partitionId) {
        this.path = properties.get("path");
        this.properties = properties;
        Path filePath = makeFilePath(path, partitionId);
        if (logger.isDebugEnabled()) {
            logger.debug("Will write to: {}", filePath);
        }
        this.contentWriter = new ContentWriter(properties);
        try {
            FileSystem fileSystem = filePath.getFileSystem(hadoopConfiguration.value());
            fileSystem.setWriteChecksum(false);
            zipOutputStream = new ZipOutputStream(fileSystem.create(filePath, true));
        } catch (IOException e) {
            throw new ConnectorException("Unable to create stream for writing zip file: " + e.getMessage(), e);
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        final String uri = row.getString(0);
        final String entryName = FileUtil.makePathFromDocumentURI(uri);
        zipOutputStream.putNextEntry(new ZipEntry(entryName));
        this.contentWriter.writeContent(row, zipOutputStream);
        zipEntryCounter++;
        if(!row.isNullAt(3) || !row.isNullAt(4) || !row.isNullAt(5) || !row.isNullAt(6)
            || !row.isNullAt(7)){
            zipOutputStream.putNextEntry(new ZipEntry(entryName+".metadata"));
            this.contentWriter.writeMetadata(row, zipOutputStream);
            zipEntryCounter++;
        }

        /**
         * Check here for non-null metadata columns. If there's at least one, call
         * DocumentRowSchema.makeDocumentMetadata(row) to get a DocumentMetadataHandle object and call toString on it.
         * That will then become an additional entry in the zip file.
         */
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(zipOutputStream);
    }

    @Override
    public WriterCommitMessage commit() {
        return new ZipCommitMessage(path, zipEntryCounter);
    }

    @Override
    public void abort() {
        // No action to take.
    }

    /**
     * Copies some of what MLCP's ArchiveWriter does, but does not create a zip file per document type. The reason
     * for that behavior in MLCP isn't known. It would not help for importing the zip files, where the URI extension will
     * determine the document type. And it seems like unnecessary zip file generation - i.e. if a user wants to export
     * 10k XML and JSON docs in the same collection, getting 2 zip files instead of 1 seems like surprising behavior.
     * Additionally, a user can arrive at that outcome if desired by using Spark to repartion the dataset based on
     * the "format" column.
     *
     * @param path
     * @param partitionId
     * @return
     */
    private Path makeFilePath(String path, int partitionId) {
        final String timestamp = new SimpleDateFormat("yyyyMMddHHmmssZ").format(new Date());
        String filePath = String.format("%s%s%s-%d.zip", path, File.separator, timestamp, partitionId);
        return new Path(filePath);
    }
}
