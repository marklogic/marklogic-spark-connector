package com.marklogic.spark.writer.file;

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
        // No messages expected yet.
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // No messages expected yet.
    }
}
