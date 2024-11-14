/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.customcode;

import com.marklogic.spark.Options;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class CustomCodeScanBuilder implements ScanBuilder {

    private CustomCodeContext context;

    public CustomCodeScanBuilder(Map<String, String> properties, StructType schema) {
        this.context = new CustomCodeContext(properties, schema, Options.READ_VARS_PREFIX);
    }

    @Override
    public Scan build() {
        return new CustomCodeScan(context);
    }
}
