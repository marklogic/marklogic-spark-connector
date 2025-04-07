/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.Options;
import com.marklogic.spark.reader.file.TripleRowSchema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

import java.util.Map;

class DocumentFileWriterFactory implements DataWriterFactory {

    static final long serialVersionUID = 1;

    private final Map<String, String> properties;
    private final SerializableConfiguration hadoopConfiguration;
    private final StructType schema;

    DocumentFileWriterFactory(Map<String, String> properties, SerializableConfiguration hadoopConfiguration, StructType schema) {
        this.properties = properties;
        this.hadoopConfiguration = hadoopConfiguration;
        this.schema = schema;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        if (this.schema.equals(TripleRowSchema.SCHEMA)) {
            return new RdfFileWriter(properties, hadoopConfiguration, partitionId);
        }

        String compression = this.properties.get(Options.WRITE_FILES_COMPRESSION);
        if (compression != null && !compression.isEmpty()) {
            return "zip".equalsIgnoreCase(compression) ?
                new ZipFileWriter(properties, hadoopConfiguration, partitionId) :
                new GzipFileWriter(properties, hadoopConfiguration);
        }

        return new DocumentFileWriter(properties, hadoopConfiguration);
    }
}
