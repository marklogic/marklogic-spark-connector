/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.client.datamovement.Forest;

import java.util.ArrayList;
import java.util.List;

class ForestPartitionPlanner {

    private final int partitionsPerForest;

    ForestPartitionPlanner(int partitionsPerForest) {
        this.partitionsPerForest = partitionsPerForest;
    }

    ForestPartition[] makePartitions(long estimate, long serverTimestamp, Forest... forests) {
        final long urisPerForest = (long) (Math.ceil((double) estimate / forests.length));
        final long urisPerPartition = (long) (Math.ceil((double) urisPerForest / partitionsPerForest));

        List<ForestPartition> partitions = new ArrayList<>();
        for (int i = 0; i < forests.length; i++) {
            Forest forest = forests[i];
            long offset = 1;
            for (int j = 0; j < partitionsPerForest; j++) {
                // If the offset for this forest exceeds the estimate across all forests, then the user has asked for
                // too many partitions. Any subsequent partition will not return any results, so we stop creating partitions.
                if (offset > estimate) {
                    break;
                }
                Long offsetEnd = j < (partitionsPerForest - 1) ? (urisPerPartition + offset - 1) : null;
                partitions.add(new ForestPartition(forest, serverTimestamp, offset, offsetEnd));
                offset += urisPerPartition;
            }
        }
        return partitions.toArray(new ForestPartition[]{});
    }
}
