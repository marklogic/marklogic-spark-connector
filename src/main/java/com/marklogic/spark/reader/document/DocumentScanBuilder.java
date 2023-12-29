package com.marklogic.spark.reader.document;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class DocumentScanBuilder implements ScanBuilder {

    private CaseInsensitiveStringMap options;

    DocumentScanBuilder(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    @Override
    public Scan build() {
        return new DocumentScan(options);
    }
}
