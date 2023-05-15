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

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamRowsTest extends AbstractIntegrationTest {

    private final static StructType SCHEMA = new StructType()
        .add("Name", DataTypes.StringType)
        .add("House", DataTypes.StringType);

    @Test
    void streamRowsFromCsvFile(@TempDir Path tempDir) throws TimeoutException {
        final String collection = "hogwarts";

        newDefaultStreamWriter(tempDir)
            .option(Options.WRITE_URI_TEMPLATE, "/hogwarts/{Name}.json")
            .option(Options.WRITE_COLLECTIONS, collection)
            .start()
            .processAllAvailable();

        assertCollectionSize(collection, 9);
        JsonNode doc = readJsonDocument("/hogwarts/Hermione Granger.json");
        assertEquals("Gryffindor", doc.get("House").asText());
        assertEquals("Hermione Granger", doc.get("Name").asText());
    }

    @Test
    void invalidTransform(@TempDir Path tempDir) throws Exception {
        StreamingQuery query = newDefaultStreamWriter(tempDir)
            .option(Options.WRITE_TRANSFORM_NAME, "this-doesnt-exist")
            .start();

        // No error will be thrown yet as the streaming occurs in the background, though we'll likely see errors
        // from the background thread. Calling processAllAvailable should force an error to occur.

        StreamingQueryException ex = assertThrows(StreamingQueryException.class, () -> query.processAllAvailable());
        assertTrue(ex.getMessage().contains("Extension this-doesnt-exist or a dependency does not exist"),
            "Unexpected error: " + ex.getCause());
    }

    private DataStreamWriter newDefaultStreamWriter(Path tempDir) {
        return newSparkSession().readStream()
            .schema(SCHEMA)
            .option("header", true)
            .format("csv")
            .load("src/test/resources/inputForStream")
            .writeStream()
            .format(CONNECTOR_IDENTIFIER)
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath())
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, "rest-extension-user,read,rest-writer,update");
    }

}
