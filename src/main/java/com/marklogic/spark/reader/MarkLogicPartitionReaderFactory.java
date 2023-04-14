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
package com.marklogic.spark.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MarkLogicPartitionReaderFactory implements PartitionReaderFactory {

    private final Logger logger = LoggerFactory.getLogger(MarkLogicPartitionReaderFactory.class);

    private PlanAnalysis planAnalysis;
    private StructType schema;
    private Map<String, String> properties;

    public MarkLogicPartitionReaderFactory(PlanAnalysis planAnalysis, StructType schema, Map<String, String> properties) {
        this.planAnalysis = planAnalysis;
        this.properties = properties;
        this.schema = schema;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        logger.info("Creating reader for partition: {}", partition);
        return new MarkLogicPartitionReader(this.planAnalysis.boundedPlan, (PlanAnalysis.Partition) partition, this.schema, this.properties);
    }
}
