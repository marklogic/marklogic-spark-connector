package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProcessStreamWithCustomCodeTest extends AbstractIntegrationTest {

    @Test
    void test(@TempDir Path tempDir) throws Exception {
        newSparkSession()
            .readStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_PARTITIONS_JAVASCRIPT, "Sequence.from([1, 2, 3])")
            .option(Options.READ_JAVASCRIPT, "Sequence.from([PARTITION])")
            .load()
            .writeStream()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_JAVASCRIPT, "declareUpdate(); var URI; " +
                "xdmp.documentInsert('/process-stream-test' + URI + '.json', {\"hello\":\"world\"}, " +
                "{\"permissions\": [xdmp.permission('spark-user-role', 'read'), xdmp.permission('spark-user-role', 'update')], " +
                "\"collections\": ['process-stream-test']});")
            .option("checkpointLocation", tempDir.toFile().getAbsolutePath())
            .start()
            .processAllAvailable();

        assertCollectionSize("process-stream-test", 3);

        // Verify 3 docs were written, one for each partition.
        for (int i = 1; i <= 3; i++) {
            JsonNode doc = readJsonDocument("/process-stream-test" + i + ".json");
            assertEquals("world", doc.get("hello").asText());
        }
    }
}
