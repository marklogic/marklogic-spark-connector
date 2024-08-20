/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

public class MarkLogicWriteBuilder implements WriteBuilder {

    private WriteContext writeContext;

    public MarkLogicWriteBuilder(WriteContext writeContext) {
        this.writeContext = writeContext;
    }

    @Override
    public Write build() {
        return new Write() {
            @Override
            public BatchWrite toBatch() {
                return new MarkLogicWrite(writeContext);
            }

            @Override
            public StreamingWrite toStreaming() {
                return new MarkLogicWrite(writeContext);
            }
        };
    }
}
