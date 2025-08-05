/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MakeFilePartitionsTest {

    @Test
    void oddNumber() {
        FilePartition[] partitions = FileUtil.makeFilePartitions(new String[]{"A", "B", "C", "D", "E"}, 3);
        assertEquals(3, partitions.length);
        assertEquals("A", partitions[0].getPaths().get(0));
        assertEquals("B", partitions[0].getPaths().get(1));
        assertEquals("C", partitions[1].getPaths().get(0));
        assertEquals("D", partitions[1].getPaths().get(1));
        assertEquals("E", partitions[2].getPaths().get(0));
    }

    @Test
    void evenPartitions() {
        FilePartition[] partitions = FileUtil.makeFilePartitions(new String[]{"A", "B", "C", "D"}, 2);
        assertEquals(2, partitions.length);
        assertEquals("A", partitions[0].getPaths().get(0));
        assertEquals("B", partitions[0].getPaths().get(1));
        assertEquals("C", partitions[1].getPaths().get(0));
        assertEquals("D", partitions[1].getPaths().get(1));
    }

    @Test
    void morePartitionsThanFiles() {
        FilePartition[] partitions = FileUtil.makeFilePartitions(new String[]{"A", "B", "C"}, 4);
        assertEquals(3, partitions.length);
        assertEquals("A", partitions[0].getPaths().get(0));
        assertEquals("B", partitions[1].getPaths().get(0));
        assertEquals("C", partitions[2].getPaths().get(0));
    }
}
