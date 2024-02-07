package com.marklogic.spark.reader.document;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

class DocumentScan implements Scan {

    private final DocumentContext context;

    DocumentScan(DocumentContext context) {
        this.context = context;
    }

    @Override
    public StructType readSchema() {
        return DocumentRowSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        return new DocumentBatch(context);
    }
}
