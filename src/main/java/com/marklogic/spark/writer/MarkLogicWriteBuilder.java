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
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

public class MarkLogicWriteBuilder implements WriteBuilder {

    private WriteContext writeContext;

    public MarkLogicWriteBuilder(WriteContext writeContext) {
        this.writeContext = writeContext;
    }

    @Deprecated
    @Override
    public BatchWrite buildForBatch() {
        return new MarkLogicWrite(writeContext);
    }

    @Override
    public StreamingWrite buildForStreaming() {
        return new MarkLogicWrite(writeContext);
    }
}
