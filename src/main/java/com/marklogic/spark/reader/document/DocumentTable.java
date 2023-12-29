package com.marklogic.spark.reader.document;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Set;

public class DocumentTable implements SupportsRead {

    private static Set<TableCapability> capabilities;

    static {
        capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new DocumentScanBuilder(options);
    }

    @Override
    public String name() {
        return "MarkLogicDocumentsTable";
    }

    @Override
    public StructType schema() {
        return DocumentRowSchema.SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return capabilities;
    }
}
