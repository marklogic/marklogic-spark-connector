package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReadGZIPFilesTest extends AbstractIntegrationTest {

    @Test
    void readThreeGZIPFiles() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .option("recursiveFileLookup", "true")
            .load("src/test/resources/gzip-files")
            .collectAsList();

        assertEquals(3, rows.size());

        verifyRow(rows.get(0), "/src/test/resources/gzip-files/hello.xml", "<hello>world</hello>\n");
        verifyRow(rows.get(1), "/src/test/resources/gzip-files/level1/hello.txt", "hello world\n");
        verifyRow(rows.get(2), "/src/test/resources/gzip-files/level1/level2/hello.json", "{\"hello\":\"world\"}\n");
    }

    @Test
    void filesNotGzipped() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load("src/test/resources/zip-files/mixed-files.zip");

        SparkException ex = assertThrows(SparkException.class, () -> dataset.count());
        assertTrue(ex.getCause() instanceof ConnectorException);
        assertTrue(ex.getCause().getMessage().startsWith("Unable to read gzip file at "),
            "Unexpected error message: " + ex.getCause().getMessage());
    }

    private void verifyRow(Row row, String expectedUriSuffix, String expectedContent) {
        String uri = row.getString(0);
        assertTrue(uri.endsWith(expectedUriSuffix), "Unexpected URI: " + uri);
        String content = new String((byte[]) row.get(3));
        assertEquals(expectedContent, content);
    }
}
