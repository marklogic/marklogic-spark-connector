/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * These tests only verify that the expected number of rows are returned, thereby verifying that the gzipped file
 * is read correctly. The files are the same as those used in ReadRdfFilesTest, just gzipped. We count on
 * ReadRdfFilesTest to verify that the triple values are correct (which really is just verifying that Jena works
 * correctly).
 */
class ReadGzipRdfFilesTest extends AbstractIntegrationTest {

    @ParameterizedTest
    @CsvSource({
        "englishlocale2.ttl.gz,32",
        "mini-taxonomy2.xml.gz,8",
        "semantics2.json.gz,12",
        "semantics2.n3.gz,25",
        "semantics2.nt.gz,8",
        "three-quads2.trig.gz,16",
        "semantics2.nq.gz,4"
    })
    void test(String file, int expectedCount) {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "rdf")
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load("src/test/resources/rdf/" + file);

        List<Row> rows = dataset.collectAsList();
        assertEquals(expectedCount, rows.size());
    }

}
