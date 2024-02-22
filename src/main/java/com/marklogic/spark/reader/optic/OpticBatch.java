/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.reader.optic;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpticBatch implements Batch {

    private static final Logger logger = LoggerFactory.getLogger(OpticBatch.class);

    private final ReadContext readContext;
    private final InputPartition[] partitions;

    OpticBatch(ReadContext readContext) {
        this.readContext = readContext;
        PlanAnalysis planAnalysis = readContext.getPlanAnalysis();
        partitions = planAnalysis != null ?
            planAnalysis.getPartitionArray() :
            new InputPartition[]{};
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating new partition reader factory");
        }
        return new OpticPartitionReaderFactory(readContext);
    }
}
