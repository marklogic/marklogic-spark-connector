package com.marklogic.spark.reader;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

public class ReadStreamOfRowsTest extends AbstractIntegrationTest {

    @Test
    void microBatch(@TempDir Path tempDir) throws Exception {
        newSparkSession()
            .readStream()
            .format("com.marklogic.spark")
            .option(Options.CLIENT_URI, makeClientUri())
            // With 3 partitions, the logging will show 3 queries to MarkLogic with a "write" occurring after each one.
            .option(ReadConstants.NUM_PARTITIONS, 3)
            .option(ReadConstants.OPTIC_DSL, "op.fromView('Medical','Authors')")
            .load()
            .writeStream()
            .format("com.marklogic.spark")
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "output")
            .option(Options.WRITE_PERMISSIONS, "spark-user-role,read,spark-user-role,update")
            .option(Options.WRITE_URI_PREFIX, "/output/")
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath())
            .start()
            .processAllAvailable();

        assertCollectionSize("output", 15);
    }
}
