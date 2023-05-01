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

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Map;

public class MarkLogicBatchWrite implements BatchWrite {
    Map<String, String> properties;
    public MarkLogicBatchWrite(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        //TODO: Look into the uses of PhysicalWriteInfo
        return new MarkLogicDataWriterFactory(properties);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
    //TODO: Need to update commit messages
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
    //TODO: Need to update reason why the job was aborted
    }
}
