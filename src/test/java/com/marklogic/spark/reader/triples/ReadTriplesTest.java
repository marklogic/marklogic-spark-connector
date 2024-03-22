package com.marklogic.spark.reader.triples;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.hadoop.fs.FileUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadTriplesTest extends AbstractIntegrationTest {

    /**
     * Maybe we need the equivalent of read_documents_* but for read_triples_*? We'll start with that.
     */
    @Test
    void test() throws IOException {
        File dir = new File("build/triples");
        FileUtil.fullyDeleteContents(dir);
        dir.mkdirs();

        Dataset<Row> dataset = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_TRIPLES_COLLECTIONS, "http://marklogic.com/semantics#default-graph")
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, 1)
            .load();

//        dataset.show();
        assertEquals(1000, dataset.count());

        dataset.repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .mode(SaveMode.Append)
            .save("build/triples");

        dataset = sparkSession.read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .load("build/triples/test.ttl");
        assertEquals(1000, dataset.count());
    }

    @Test
    void adHoc() {
        Dataset<Row> dataset = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, "admin:admin@localhost:8115")
            .option(Options.READ_TRIPLES_COLLECTIONS, "http://marklogic.com/semantics#default-graph")
            .option(Options.READ_DOCUMENTS_PARTITIONS_PER_FOREST, 1)
            .option(Options.READ_BATCH_SIZE, 10)
            .load();

        dataset.show();
        logger.info("COUNT: " + dataset.count());
    }
}
