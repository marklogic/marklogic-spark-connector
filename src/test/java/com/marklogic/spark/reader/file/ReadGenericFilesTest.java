package com.marklogic.spark.reader.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The generic file reader has support for aborting or continuing on failure - but we haven't yet found a way to
 * force an error to occur. An encoding issue doesn't cause an error because the reader simply reads in all the
 * bytes from the file.
 */
class ReadGenericFilesTest extends AbstractIntegrationTest {

    @Test
    void readAndWriteMixedFiles() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_NUM_PARTITIONS, 2)
            .load("src/test/resources/mixed-files");

        List<Row> rows = dataset.collectAsList();
        assertEquals(4, rows.size());
        rows.forEach(row -> {
            assertFalse(row.isNullAt(0)); // URI
            assertFalse(row.isNullAt(1)); // content
            Stream.of(2, 3, 4, 5, 6, 7).forEach(index -> assertTrue(row.isNullAt(index),
                "Expecting a null value for every column that isn't URI or content; index: " + index));
        });

        defaultWrite(dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "generic")
            .option(Options.WRITE_URI_REPLACE, ".*/mixed-files,''"));

        JsonNode doc = readJsonDocument("/hello.json", "generic");
        assertEquals("world", doc.get("hello").asText());
        XmlNode xmlDoc = readXmlDocument("/hello.xml", "generic");
        xmlDoc.assertElementValue("/hello", "world");
        String text = getDatabaseClient().newTextDocumentManager().read("/hello.txt", new StringHandle()).get();
        assertEquals("hello world", text.trim());
        BytesHandle handle = getDatabaseClient().newBinaryDocumentManager().read("/hello2.txt.gz", new BytesHandle());
        assertEquals(Format.BINARY, handle.getFormat());
    }
}
