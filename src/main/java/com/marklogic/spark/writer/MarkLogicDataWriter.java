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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JacksonGenerator;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.UUID;

class MarkLogicDataWriter implements DataWriter<InternalRow> {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicDataWriter.class);

    private final WriteContext writeContext;
    private final DatabaseClient databaseClient;
    private final JSONDocumentManager documentManager;
    private final DocumentMetadataHandle documentMetadata;
    private final int partitionId;
    private final long taskId;

    private DocumentWriteSet writeSet;

    MarkLogicDataWriter(WriteContext writeContext, int partitionId, long taskId) {
        this.writeContext = writeContext;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.databaseClient = writeContext.connectToMarkLogic();
        this.documentManager = this.databaseClient.newJSONDocumentManager();
        // TODO Make this all configurable
        this.documentMetadata = new DocumentMetadataHandle()
            .withCollections("my-test-data")
            .withPermission("spark-user-role", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE);
    }

    @Override
    public void write(InternalRow record) {
        if (writeSet == null) {
            writeSet = documentManager.newWriteSet();
            writeSet.addDefault(documentMetadata);
        }
        // TODO Make the URI configurable, like in MLCP
        final String uri = "/test/" + UUID.randomUUID() + ".json";
        // TODO Figure out how to support XML/TXT.
        String json = convertRowToJSONString(record);
        writeSet.add(uri, new StringHandle(json).withFormat(Format.JSON));

        // TODO Do we need a batchSize parameter here to force a write to occur? Or can the user control that via
        // some Spark-specific option for when a commit occurs?
    }

    @Override
    public WriterCommitMessage commit() {
        final int docCount = writeSet.size();
        if (logger.isDebugEnabled()) {
            logger.debug("Committing; partitionId: {}; taskId: {}; document count: {}; ", partitionId, taskId, docCount);
        }
        this.documentManager.write(this.writeSet);
        this.writeSet = null;
        return new MarkLogicCommitMessage(docCount, partitionId, taskId);
    }

    @Override
    public void abort() {
        logger.error("Abort called");
        this.databaseClient.release();
    }

    @Override
    public void close() {
        //TODO : Need to log the messages accumulated while committing and aborting a transaction.

    }

    private String convertRowToJSONString(InternalRow record) {
        StringWriter jsonObjectWriter = new StringWriter();
        JacksonGenerator jacksonGenerator = new JacksonGenerator(
            this.writeContext.getSchema(),
            jsonObjectWriter,
            Util.DEFAULT_JSON_OPTIONS
        );
        jacksonGenerator.write(record);
        jacksonGenerator.flush();
        return jsonObjectWriter.toString();
    }

    private static class MarkLogicCommitMessage implements WriterCommitMessage {
        private int docCount;
        private int partitionId;
        private long taskId;

        public MarkLogicCommitMessage(int docCount, int partitionId, long taskId) {
            this.docCount = docCount;
            this.partitionId = partitionId;
            this.taskId = taskId;
        }

        @Override
        public String toString() {
            return String.format("partitionId: %d; taskId: %d; docCount: %d", partitionId, taskId, docCount);
        }
    }
}
