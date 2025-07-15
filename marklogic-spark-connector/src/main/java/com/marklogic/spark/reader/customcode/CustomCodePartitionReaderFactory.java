/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.customcode;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

class CustomCodePartitionReaderFactory implements PartitionReaderFactory {

    static final long serialVersionUID = 1;

    private CustomCodeContext customCodeContext;

    public CustomCodePartitionReaderFactory(CustomCodeContext customCodeContext) {
        this.customCodeContext = customCodeContext;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new CustomCodePartitionReader(customCodeContext, ((CustomCodePartition) partition).getPartition());
    }
}
