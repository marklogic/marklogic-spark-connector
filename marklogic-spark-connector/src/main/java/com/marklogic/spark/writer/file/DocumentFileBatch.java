/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

import java.util.Map;

class DocumentFileBatch implements BatchWrite {

    private final Map<String, String> properties;
    private final StructType schema;

    DocumentFileBatch(Map<String, String> properties, StructType schema) {
        this.properties = properties;
        this.schema = schema;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        // This is the last chance we have for accessing the hadoop config, which is needed by the writer.
        // SerializableConfiguration allows for it to be sent to the factory.
        Configuration config = SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
        return new DocumentFileWriterFactory(properties, new SerializableConfiguration(config), schema);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (Util.MAIN_LOGGER.isInfoEnabled() && messages != null && messages.length > 0) {
            int fileCount = 0;
            int zipFileCount = 0;
            int zipEntryCount = 0;
            String path = null;
            String zipFilePath = null;
            for (WriterCommitMessage message : messages) {
                if (message instanceof FileCommitMessage fileCommitMessage) {
                    path = fileCommitMessage.getPath();
                    fileCount += fileCommitMessage.getFileCount();
                } else if (message instanceof ZipCommitMessage zipCommitMessage) {
                    path = zipCommitMessage.getPath();
                    zipFilePath = zipCommitMessage.getZipFilePath();
                    zipFileCount++;
                    zipEntryCount += zipCommitMessage.getZipEntryCount();
                }
            }
            if (fileCount == 1) {
                Util.MAIN_LOGGER.info("Wrote 1 file to: {}.", path);
            } else if (fileCount > 1) {
                Util.MAIN_LOGGER.info("Wrote {} files to: {}.", fileCount, path);
            } else if (zipFileCount == 1) {
                Util.MAIN_LOGGER.info("Wrote 1 zip file containing {} entries to: {}.", zipEntryCount, zipFilePath);
            } else if (zipFileCount > 1) {
                Util.MAIN_LOGGER.info("Wrote {} zip files containing a total of {} entries to: {}.", zipFileCount, zipEntryCount, path);
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // No messages expected yet.
    }
}
