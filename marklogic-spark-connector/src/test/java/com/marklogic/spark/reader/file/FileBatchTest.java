/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileBatchTest extends AbstractIntegrationTest {

    @Test
    void moreFilesThanDefaultParallelism() {
        final int defaultParallelism = newSparkSession().sparkContext().defaultParallelism();

        final List<String> filePaths = new ArrayList<>();
        for (int i = 0; i < defaultParallelism * 2; i++) {
            filePaths.add("file" + i + ".txt");
        }

        int defaultPartitions = FileBatch.getDefaultNumberOfPartitions(filePaths);
        assertEquals(defaultParallelism, defaultPartitions, "When the number of file paths exceeds the default " +
            "parallelism value for Spark, the default parallelism should be used. This should provide good " +
            "performance and will avoid creating too many small partitions.");
    }

    @Test
    void fewerFilesThanDefaultParallelism() {
        newSparkSession(); // just to ensure Spark context is initialized
        
        final List<String> filePaths = Arrays.asList("file0.txt");

        int defaultPartitions = FileBatch.getDefaultNumberOfPartitions(filePaths);
        assertEquals(1, defaultPartitions, "When the number of file paths is less than the default " +
            "parallelism value for Spark, the number of file paths should be used as the number of partitions. " +
            "This will help ensure that all files are processed in parallel.");
    }
}
