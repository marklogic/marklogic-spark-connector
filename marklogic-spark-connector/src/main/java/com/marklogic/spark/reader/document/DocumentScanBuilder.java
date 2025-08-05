/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.spark.Util;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class DocumentScanBuilder implements ScanBuilder, SupportsPushDownLimit {

    private final DocumentContext context;

    DocumentScanBuilder(CaseInsensitiveStringMap options, StructType schema) {
        this.context = new DocumentContext(options, schema);
        if (this.context.isStreamingFiles() && Util.MAIN_LOGGER.isInfoEnabled()) {
            Util.MAIN_LOGGER.info("File streaming is enabled; will read documents from MarkLogic during writer phase.");
        }
    }

    @Override
    public Scan build() {
        return new DocumentScan(context);
    }

    @Override
    public boolean pushLimit(int limit) {
        this.context.setLimit(limit);
        return true;
    }

    @Override
    public boolean isPartiallyPushed() {
        // A partition reader can only ensure that it doesn't exceed the limit. In a worst case scenario, every reader
        // will return "limit" rows. So must return true here to ensure that Spark reduces the dataset to the
        // appropriate limit.
        return true;
    }
}
