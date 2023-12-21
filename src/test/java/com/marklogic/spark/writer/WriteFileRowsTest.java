package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WriteFileRowsTest extends AbstractWriteTest {

    @Test
    void writeAllFourDocumentTypes() {
        newSparkSession()
            .read()
            .format("binaryFile")
            .load("src/test/resources/mixed-files/")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "mixed-files")
            .mode(SaveMode.Append)
            .save();

        DatabaseClient client = getDatabaseClient();
        List<String> uris = getUrisInCollection("mixed-files", 4);
        for (String uri : uris) {
            assertTrue(uri.contains("src/test/resources/mixed-files"), "The URI is expected to have the full file " +
                "path in it by default.");
            if (uri.endsWith("/hello.json")) {
                JacksonHandle handle = client.newJSONDocumentManager().read(uri, new JacksonHandle());
                assertEquals(Format.JSON, handle.getFormat());
                assertEquals("world", handle.get().get("hello").asText());
            } else if (uri.endsWith("/hello.txt")) {
                StringHandle handle = client.newTextDocumentManager().read(uri, new StringHandle());
                assertEquals(Format.TEXT, handle.getFormat());
                assertEquals("hello world\n", handle.get());
            } else if (uri.endsWith("/hello.xml")) {
                StringHandle handle = client.newTextDocumentManager().read(uri, new StringHandle());
                assertEquals(Format.XML, handle.getFormat());
                assertTrue(handle.get().contains("<hello>world</hello>"));
            } else if (uri.endsWith("/hello2.txt.gz")) {
                assertEquals(Format.BINARY, client.newDocumentManager().read(uri, new InputStreamHandle()).getFormat());
            } else {
                fail("Unexpected URI: " + uri);
            }
        }
    }

    @Test
    void uriTemplate() {
        File f = new File("src/test/resources/mixed-files/hello.json");

        Dataset<Row> dataset = newSparkSession()
            .read()
            .format("binaryFile")
            .load("src/test/resources/mixed-files/*.json");

        Row row = dataset.collectAsList().get(0);
        // For as-yet unknown reasons, the timestamp in an InternalRow - which the connector writer receives - will
        // have 000 appended to it, thus capturing microseconds. But a Row will have the same value that the JVM
        // returns when getting lastModified for a File. This does not seem like an issue for a user, we just need to
        // account for it in our test.
        final String expectedURI = String.format("/testfile/%d000/%d.json", row.getTimestamp(1).getTime(), row.getLong(2));

        dataset
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_COLLECTIONS, "template-test")
            .option(Options.WRITE_URI_TEMPLATE, "/testfile/{modificationTime}/{length}.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument(expectedURI);
        assertEquals("world", doc.get("hello").asText());
    }

}
