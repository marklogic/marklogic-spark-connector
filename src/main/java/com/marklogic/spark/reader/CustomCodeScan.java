package com.marklogic.spark.reader;

import com.marklogic.spark.CustomCodeContext;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

class CustomCodeScan implements Scan {

    private CustomCodeContext customCodeContext;

    public CustomCodeScan(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
    }

    @Override
    public StructType readSchema() {
        return customCodeContext.getSchema();
    }

    @Override
    public Batch toBatch() {
        return new CustomCodeBatch(customCodeContext);
    }
}
