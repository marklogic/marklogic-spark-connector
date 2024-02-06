package com.marklogic.spark.reader.document;

import com.marklogic.client.datamovement.Forest;
import com.marklogic.client.datamovement.impl.ForestImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MakeForestPartitionsTest {

    private final long FAKE_SERVER_TIMESTAMP = 100;

    ForestPartition[] partitions;

    @Test
    void twoForests() {
        partitions = new ForestPartitionPlanner(3).makePartitions(300000, FAKE_SERVER_TIMESTAMP,
            forest("f1"), forest("f2"));
        assertEquals(6, partitions.length);

        verifyPartition(0, "f1", 1, 50000);
        verifyPartition(1, "f1", 50001, 100000);
        verifyPartition(2, "f1", 100001, null);
        verifyPartition(3, "f2", 1, 50000);
        verifyPartition(4, "f2", 50001, 100000);
        verifyPartition(5, "f2", 100001, null);
    }

    @Test
    void threeForests() {
        partitions = new ForestPartitionPlanner(3).makePartitions(300000, FAKE_SERVER_TIMESTAMP,
            forest("f1"), forest("f2"), forest("f3"));
        assertEquals(9, partitions.length);

        verifyPartition(0, "f1", 1, 33334);
        verifyPartition(1, "f1", 33335, 66668);
        verifyPartition(2, "f1", 66669, null);
        verifyPartition(3, "f2", 1, 33334);
        verifyPartition(4, "f2", 33335, 66668);
        verifyPartition(5, "f2", 66669, null);
        verifyPartition(6, "f3", 1, 33334);
        verifyPartition(7, "f3", 33335, 66668);
        verifyPartition(8, "f3", 66669, null);
    }

    @Test
    void onePartition() {
        partitions = new ForestPartitionPlanner(1).makePartitions(300000, FAKE_SERVER_TIMESTAMP,
            forest("f1"), forest("f2"), forest("f3"));
        assertEquals(3, partitions.length);

        verifyPartition(0, "f1", 1, null);
        verifyPartition(1, "f2", 1, null);
        verifyPartition(2, "f3", 1, null);
    }

    @Test
    void farTooManyPartitions() {
        partitions = new ForestPartitionPlanner(200).makePartitions(100, FAKE_SERVER_TIMESTAMP, forest("f1"), forest("f2"));
        assertEquals(200, partitions.length, "If the user asks for more partitions than there are results, the " +
            "planner is expected to create at most N partitions per forest, where N is 100 since that's the estimate " +
            "of all matching results.");
    }

    private Forest forest(String name) {
        return new ForestImpl(null, null, null, null, null, name, null, false, false);
    }

    private void verifyPartition(int partitionIndex, String forestName, int offsetStart, Integer offsetEnd) {
        ForestPartition partition = partitions[partitionIndex];
        assertEquals(forestName, partition.getForestName());
        assertEquals(FAKE_SERVER_TIMESTAMP, partition.getServerTimestamp());
        assertEquals(new Long(offsetStart), partition.getOffsetStart());
        assertEquals(offsetEnd == null ? null : new Long(offsetEnd), partition.getOffsetEnd());
    }
}
