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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamRowsTest extends AbstractIntegrationTest {
    StructType struct = new StructType()
        .add("Name", DataTypes.StringType)
        .add("House", DataTypes.StringType);

    @Test
    void streamRowsFromCsvFile(@TempDir Path tempDir) throws TimeoutException {

        newDefaultStreamWriter(newSparkSession(), "src/test/resources/inputForStream", struct, "csv")
            .outputMode("append")
            .option(Options.WRITE_COLLECTIONS, "my-test-data")
            // checkpointLocation needs to be a new location everytime job is run else docs are not written.
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath()).start().processAllAvailable();
        final int expectedCollectionSize = 9;
        String uri = getUrisInCollection("my-test-data", expectedCollectionSize).get(0);
        JsonNode doc = readJsonDocument(uri);
        assertTrue(doc.has("Name"));
        assertTrue(doc.has("House"));
    }

    @Test
    void inValidTransform(@TempDir Path tempDir) {
        try{
            newDefaultStreamWriter(newSparkSession(), "src/test/resources/inputForStream", struct, "csv")
                .outputMode("append")
                .option(Options.WRITE_COLLECTIONS, "my-test-data")
                .option(Options.WRITE_TRANSFORM_NAME, "this-doesnt-exist")
                .option("checkpointLocation", tempDir.toFile().getAbsolutePath()) // checkpointLocation needs to be a new location everytime job is run else docs are not written.
                .start().processAllAvailable();
        } catch (Exception ex){
            assertTrue(ex.getMessage().contains("XDMP-MODNOTFOUND: (err:XQST0059) Module /marklogic.rest.transform/this-doesnt-exist/assets/transform.xqy not found"));
        }
    }
}
