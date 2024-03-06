package com.marklogic.spark.writer.file;

import com.marklogic.spark.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.util.SerializableConfiguration;

import java.util.Map;

class DocumentFileBatch implements BatchWrite {

    private final Map<String, String> properties;

    DocumentFileBatch(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        // This is the last chance we have for accessing the hadoop config, which is needed by the writer.
        // SerializableConfiguration allows for it to be sent to the factory.
        Configuration config = SparkSession.active().sparkContext().hadoopConfiguration();
        return new DocumentFileWriterFactory(properties, new SerializableConfiguration(config));
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (Util.MAIN_LOGGER.isInfoEnabled() && messages != null && messages.length > 0) {
            int fileCount = 0;
            int zipFileCount = 0;
            int zipEntryCount = 0;
            String path = null;
            for (WriterCommitMessage message : messages) {
                if (message instanceof FileCommitMessage) {
                    path = ((FileCommitMessage)message).getPath();
                    fileCount += ((FileCommitMessage) message).getFileCount();
                } else if (message instanceof ZipCommitMessage) {
                    path = ((ZipCommitMessage)message).getPath();
                    zipFileCount++;
                    zipEntryCount += ((ZipCommitMessage) message).getZipEntryCount();
                }
            }
            if (fileCount == 1) {
                Util.MAIN_LOGGER.info("Wrote 1 file to {}.", path);
            } else if (fileCount > 1) {
                Util.MAIN_LOGGER.info("Wrote {} files to {}.", fileCount, path);
            } else if (zipFileCount == 1) {
                Util.MAIN_LOGGER.info("Wrote 1 zip file containing {} entries to {}.", zipEntryCount, path);
            } else if (zipFileCount > 1) {
                Util.MAIN_LOGGER.info("Wrote {} zip files containing a total of {} entries to {}.", zipFileCount, zipEntryCount, path);
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // No messages expected yet.
    }
}
