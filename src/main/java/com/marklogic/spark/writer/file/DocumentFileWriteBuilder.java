package com.marklogic.spark.writer.file;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

import java.util.Map;

public class DocumentFileWriteBuilder implements WriteBuilder {

    private final Map<String, String> properties;

    public DocumentFileWriteBuilder(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Write build() {
        return new Write() {
            @Override
            public BatchWrite toBatch() {
                return new DocumentFileBatch(properties);
            }
        };
    }
}
