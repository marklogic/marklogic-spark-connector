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

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadStreamOfRowsTest extends AbstractIntegrationTest {

    @Test
    void readAndWriteMicroBatches(@TempDir Path tempDir) throws Exception {
        newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            // With 3 partitions, the logging will show 3 queries to MarkLogic with a "write" occurring after each one.
            .option(Options.READ_NUM_PARTITIONS, 3)
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors')")
            .load()
            .writeStream()
            .format(CONNECTOR_IDENTIFIER)
            .options(defaultWriteOptions())
            .option(Options.WRITE_COLLECTIONS, "output")
            .option(Options.WRITE_URI_PREFIX, "/output/")
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath())
            .start()
            .processAllAvailable();

        assertCollectionSize("output", 15);
    }

    @Test
    void readAndWriteStreamToMemory() throws Exception {
        AtomicInteger microBatchCounter = new AtomicInteger();
        AtomicLong rowCount = new AtomicLong();

        newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_NUM_PARTITIONS, 2)
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors')")
            .load()
            .writeStream()
            .format("memory")
            .option("queryName", "anything works here")
            .foreachBatch((dataset, batchId) -> {
                rowCount.addAndGet(dataset.count());
                microBatchCounter.incrementAndGet();
            })
            .start()
            .processAllAvailable();

        assertEquals(15, rowCount.get());
        assertEquals(2, microBatchCounter.get(), "Expecting 2 micro batches, as 2 buckets should have been created " +
            "based on the partition count and batch size (there is a small chance that every row ended up in one " +
            "bucket, but that should be extremely rare).");
    }

    @Test
    void readWithNoQuery() {
        DataStreamReader reader = newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri());

        ConnectorException ex = assertThrows(ConnectorException.class, () -> reader.load());
        assertEquals("No Optic query found; must define spark.marklogic.read.opticQuery", ex.getMessage());
    }
}
