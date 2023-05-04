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
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.util.Map;

public class MarkLogicDataWriter implements DataWriter<InternalRow> {
    Map<String, String> map;
    public MarkLogicDataWriter(Map<String, String> map) {
        this.map = map;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        DatabaseClient client = DatabaseClientFactory.newClient(propertyName -> map.get("spark." + propertyName));
        TextDocumentManager textDocumentManager = client.newTextDocumentManager();
        DocumentWriteSet batch = textDocumentManager.newWriteSet();
        DocumentMetadataHandle defaultMetadata =
            new DocumentMetadataHandle().withCollections("my-test-data");
        // TODO: modify below to use https://github.com/marklogic/marklogic-data-hub/blob/develop/marklogic-data-hub-spark-connector/src/main/java/com/marklogic/hub/spark/sql/sources/v2/writer/HubDataWriter.java#L111
        batch.add("doc"+record.getString(0)+".txt",
            new StringHandle(record.getString(0)+" "+record.getString(1)).withFormat(Format.TEXT));
        batch.addDefault(defaultMetadata);
        textDocumentManager.write(batch);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        //TODO: Look into the uses of WriterCommitMessage
        return null;
    }

    @Override
    public void abort() {
        //TODO: What error is good to throw here?
        throw new UnsupportedOperationException("Transaction cannot be aborted");
    }

    @Override
    public void close() throws IOException {
        //TODO : Need to log the messages accumulated while committing and aborting a transaction.

    }
}
