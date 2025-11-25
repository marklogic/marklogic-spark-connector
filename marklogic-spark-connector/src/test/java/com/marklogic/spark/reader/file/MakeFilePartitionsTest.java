/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MakeFilePartitionsTest {

    @Test
    void oddNumber() {
        FilePartition[] partitions = FileUtil.makeFilePartitions(new String[]{"A", "B", "C", "D", "E"}, 3);
        verifyNoPartitionIsEmpty(partitions);

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
        verifyNoPartitionIsEmpty(partitions);

        assertEquals(2, partitions.length);
        assertEquals("A", partitions[0].getPaths().get(0));
        assertEquals("B", partitions[0].getPaths().get(1));
        assertEquals("C", partitions[1].getPaths().get(0));
        assertEquals("D", partitions[1].getPaths().get(1));
    }

    @Test
    void morePartitionsThanFiles() {
        FilePartition[] partitions = FileUtil.makeFilePartitions(new String[]{"A", "B", "C"}, 4);
        verifyNoPartitionIsEmpty(partitions);

        assertEquals(3, partitions.length);
        assertEquals("A", partitions[0].getPaths().get(0));
        assertEquals("B", partitions[1].getPaths().get(0));
        assertEquals("C", partitions[2].getPaths().get(0));
    }

    @Test
    void zeroPartitions() {
        FilePartition[] partitions = FileUtil.makeFilePartitions(new String[]{"A", "B", "C"}, 0);
        verifyNoPartitionIsEmpty(partitions);

        assertEquals(1, partitions.length, "If a value less than 1 is passed in, partitions should default to 1.");
    }

    @Test
    void distributeFilesEvenly() {
        // For bug MLE-25626
        FilePartition[] partitions = FileUtil.makeFilePartitions(new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"}, 8);
        verifyNoPartitionIsEmpty(partitions);

        assertEquals(8, partitions.length);
        assertEquals(2, partitions[0].getPaths().size());
        assertEquals(2, partitions[1].getPaths().size());
        assertEquals(2, partitions[2].getPaths().size());
        assertEquals(2, partitions[3].getPaths().size());
        assertEquals(1, partitions[4].getPaths().size());
        assertEquals(1, partitions[5].getPaths().size());
        assertEquals(1, partitions[6].getPaths().size());
        assertEquals(1, partitions[7].getPaths().size());
    }

    @Test
    void equalFilesAndPartitions() {
        FilePartition[] partitions = FileUtil.makeFilePartitions(new String[]{"A", "B", "C", "D", "E", "F", "G", "H"}, 8);
        verifyNoPartitionIsEmpty(partitions);

        assertEquals(8, partitions.length);
        for (int i = 0; i < partitions.length; i++) {
            assertEquals(1, partitions[i].getPaths().size());
        }
    }

    @Test
    void singleFile() {
        FilePartition[] partitions = FileUtil.makeFilePartitions(new String[]{"A"}, 1);
        verifyNoPartitionIsEmpty(partitions);

        assertEquals(1, partitions.length);
        assertEquals(1, partitions[0].getPaths().size());
        assertEquals("A", partitions[0].getPaths().get(0));
    }

    private void verifyNoPartitionIsEmpty(FilePartition[] partitions) {
        for (FilePartition partition : partitions) {
            assertFalse(partition.getPaths().isEmpty(), "No partition should be empty");
        }
    }
}
