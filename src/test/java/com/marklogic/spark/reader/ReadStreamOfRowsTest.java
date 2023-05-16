package com.marklogic.spark.reader;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReadStreamOfRowsTest extends AbstractIntegrationTest {

    @Test
    void readAndWriteMicroBatches(@TempDir Path tempDir) throws Exception {
        newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            // With 3 partitions, the logging will show 3 queries to MarkLogic with a "write" occurring after each one.
            .option(Options.READ_NUM_PARTITIONS, 3)
            .option(Options.READ_OPTIC_DSL, "op.fromView('Medical','Authors')")
            .load()
            .writeStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "output")
            .option(Options.WRITE_PERMISSIONS, "spark-user-role,read,spark-user-role,update")
            .option(Options.WRITE_URI_PREFIX, "/output/")
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath())
            .start()
            .processAllAvailable();

        assertCollectionSize("output", 15);
    }

    @Test
    void readAndWriteStreamToMemory(@TempDir Path tempDir) throws Exception {
        AtomicInteger microBatchCounter = new AtomicInteger();
        AtomicLong rowCount = new AtomicLong();

        newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_NUM_PARTITIONS, 2)
            .option(Options.READ_OPTIC_DSL, "op.fromView('Medical','Authors')")
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
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> newSparkSession()
                .readStream()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.CLIENT_URI, makeClientUri())
                .load()
        );

        assertEquals("No Optic query found; must define spark.marklogic.read.opticDsl", ex.getMessage());
    }
}
