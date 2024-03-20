package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadDelimitedTextFilesTest extends AbstractIntegrationTest {

    @Test
    void test() throws Exception {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "delimited_text")
            .load("src/test/resources/data.csv")
            .show();

        List<Row> rows = newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "delimited_text")
            .load("src/test/resources/data.csv")
            .collectAsList();

        assertEquals(200, rows.size());

        JsonNode doc = objectMapper.readTree((byte[]) rows.get(0).get(1));
        System.out.println(rows.get(0).getString(0));
        System.out.println(doc.toPrettyString());
    }
}
