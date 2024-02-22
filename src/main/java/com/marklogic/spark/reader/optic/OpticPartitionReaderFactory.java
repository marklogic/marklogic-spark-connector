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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpticPartitionReaderFactory implements PartitionReaderFactory {

    static final long serialVersionUID = 1;

    private static final Logger logger = LoggerFactory.getLogger(OpticPartitionReaderFactory.class);
    private final ReadContext readContext;

    OpticPartitionReaderFactory(ReadContext readContext) {
        this.readContext = readContext;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating reader for partition: {}", partition);
        }
        return new OpticPartitionReader(this.readContext, (PlanAnalysis.Partition) partition);
    }
}
