package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;

class ReadJsonLinesFilesTest extends AbstractIntegrationTest {

    @Test
    void test() {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "json_lines")
            .load("src/test/resources/json-lines")
            .show();
    }
}
