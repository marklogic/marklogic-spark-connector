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
package com.marklogic.spark.writer;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.util.SerializableConfiguration;

class WriteBatcherDataWriterFactory implements DataWriterFactory, StreamingDataWriterFactory {

    private final WriteContext writeContext;
    private final SerializableConfiguration hadoopConfiguration;

    WriteBatcherDataWriterFactory(WriteContext writeContext, SerializableConfiguration hadoopConfiguration) {
        this.writeContext = writeContext;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new WriteBatcherDataWriter(writeContext, hadoopConfiguration, partitionId);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return createWriter(partitionId, taskId);
    }
}
