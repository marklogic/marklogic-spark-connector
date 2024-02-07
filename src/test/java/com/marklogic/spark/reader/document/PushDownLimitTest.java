package com.marklogic.spark.reader.document;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PushDownLimitTest extends AbstractIntegrationTest {

    @Test
    void two() {
        long count = readAuthors().limit(2).count();
        assertTrue(count <= 6, "With a limit of 2, each reader should read at most 2 docs; they can't do " +
            "any fewer than that because each one has no idea how many documents any other reader will get. " +
            "Unexpected count: " + count);
    }

    @Test
    void zero() {
        long count = readAuthors().limit(0).count();
        assertEquals(0, count);
    }

    @Test
    void limitIsMoreThanTotal() {
        long count = readAuthors().limit(20).count();
        assertEquals(15, count, "A limit greater than then number of matching documents has no impact on the results.");
    }

    private Dataset<Row> readAuthors() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            // Using a single partition to increase the chance that a reader will hit the limit.
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, 1)
            .load();
    }

}
