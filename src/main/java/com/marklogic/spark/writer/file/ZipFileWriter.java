package com.marklogic.spark.writer.file;

import com.marklogic.spark.ConnectorException;
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

    private ZipOutputStream zipOutputStream;

    ZipFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration, int partitionId) {
        Path path = makeFilePath(properties, partitionId);
        if (logger.isDebugEnabled()) {
            logger.debug("Will write to: {}", path);
        }
        try {
            FileSystem fileSystem = path.getFileSystem(hadoopConfiguration.value());
            fileSystem.setWriteChecksum(false);
            zipOutputStream = new ZipOutputStream(fileSystem.create(path, true));
        } catch (IOException e) {
            throw new ConnectorException("Unable to create stream for writing zip file: " + e.getMessage(), e);
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        final String uri = row.getString(0);
        final String entryName = FileUtil.makePathFromDocumentURI(uri);
        zipOutputStream.putNextEntry(new ZipEntry(entryName));
        zipOutputStream.write(row.getBinary(1));
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
        return null;
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
     * @param properties
     * @param partitionId
     * @return
     */
    private Path makeFilePath(Map<String, String> properties, int partitionId) {
        final String timestamp = new SimpleDateFormat("yyyyMMddHHmmssZ").format(new Date());
        String path = String.format("%s%s%s-%d.zip", properties.get("path"), File.separator, timestamp, partitionId);
        return new Path(path);
    }
}
