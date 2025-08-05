/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Set;

/**
 * For reading and writing rows conforming to {@code DocumentRowSchema}.
 */
public class DocumentTable implements SupportsRead, SupportsWrite {

    private static Set<TableCapability> capabilities;

    static {
        capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        capabilities.add(TableCapability.BATCH_WRITE);
    }

    private final StructType schema;

    public DocumentTable(StructType schema) {
        this.schema = schema;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new DocumentScanBuilder(options, this.schema);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return null;
    }

    @Override
    public String name() {
        return "MarkLogicDocumentTable";
    }

    @Override
    // Spark has deprecated this, but it must still be implemented.
    @SuppressWarnings("deprecation")
    public StructType schema() {
        return this.schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return capabilities;
    }
}
