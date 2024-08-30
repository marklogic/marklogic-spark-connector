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

        /**
         * How does a user ask for aggregate JSON? They just need to say they want it. Don't think there's any
         * config decision they need to make.
         * spark.marklogic.write.aggregates.json=true?
         */
        if ("true".equals(this.properties.get(Options.WRITE_AGGREGATES_JSON))) {
            return new AggregateJsonFileWriter(properties, hadoopConfiguration, partitionId);
        }
        
        String xmlElement = this.properties.get(Options.WRITE_AGGREGATES_XML_ELEMENT);
        if (xmlElement != null && xmlElement.trim().length() > 0) {
            return new AggregateXmlFileWriter(properties, hadoopConfiguration, partitionId);
        }

        String compression = this.properties.get(Options.WRITE_FILES_COMPRESSION);
        if (compression != null && compression.length() > 0) {
            if ("zip".equalsIgnoreCase(compression)) {
                return new ZipFileWriter(properties, hadoopConfiguration, partitionId);
            }
            return new GzipFileWriter(properties, hadoopConfiguration);
        }
        return new DocumentFileWriter(properties, hadoopConfiguration);
    }
}
