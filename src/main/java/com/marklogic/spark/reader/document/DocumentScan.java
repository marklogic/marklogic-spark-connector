package com.marklogic.spark.reader.document;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class DocumentScan implements Scan {

    private CaseInsensitiveStringMap options;

    DocumentScan(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return DocumentRowSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        return new DocumentBatch(options);
    }
}
