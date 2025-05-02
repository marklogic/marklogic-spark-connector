/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class WriteFileRowsTest extends AbstractWriteTest {

    @Test
    void writeAllFourDocumentTypes() {
        defaultWrite(readFourBinaryFilesAndWrite()
            .option(Options.WRITE_COLLECTIONS, "mixed-files")
            .option(Options.WRITE_URI_REPLACE, ".*/resources,''")
        );

        DatabaseClient client = getDatabaseClient();
        assertCollectionSize("mixed-files", 4);

        JacksonHandle jsonHandle = client.newJSONDocumentManager().read("/mixed-files/hello.json", new JacksonHandle());
        assertEquals(Format.JSON, jsonHandle.getFormat());
        assertEquals("world", jsonHandle.get().get("hello").asText());

        StringHandle textHandle = client.newTextDocumentManager().read("/mixed-files/hello.txt", new StringHandle());
        assertEquals(Format.TEXT, textHandle.getFormat());
        assertEquals("hello world\n", textHandle.get());

        StringHandle xmlHandle = client.newTextDocumentManager().read("/mixed-files/hello.xml", new StringHandle());
        assertEquals(Format.XML, xmlHandle.getFormat());
        assertTrue(xmlHandle.get().contains("<hello>world</hello>"));

        assertEquals(Format.BINARY, client.newDocumentManager().read("/mixed-files/hello2.txt.gz", new InputStreamHandle()).getFormat());
    }

    @Test
    void multipleUriReplacePatterns() {
        defaultWrite(readFourBinaryFilesAndWrite()
            .option(Options.WRITE_COLLECTIONS, "multiple-replace")
            .option(Options.WRITE_URI_REPLACE, ".*/src/test,'/test',resources,'modified'")
        );

        List<String> uris = getUrisInCollection("multiple-replace", 4);
        Stream.of("/test/modified/mixed-files/hello.txt", "/test/modified/mixed-files/hello.json",
            "/test/modified/mixed-files/hello.txt", "/test/modified/mixed-files/hello2.txt.gz").forEach(uri -> {
            assertTrue(uris.contains(uri), String.format("Did not find %s in %s", uri, uris));
        });
    }

    @Test
    void invalidReplacePattern() {
        DataFrameWriter writer = readFourBinaryFilesAndWrite()
            .option(Options.WRITE_URI_REPLACE, ".*/src/test,'/test',resources")
            .mode(SaveMode.Append);

        SparkException ex = assertThrows(SparkException.class, writer::save);
        assertTrue(ex.getCause() instanceof ConnectorException);
        assertEquals("The URI replacement expression must contain an equal number of patterns and replacement strings: .*/src/test,'/test',resources",
            ex.getCause().getMessage());
    }

    @Test
    void replacePatternMissingSingleQuotes() {
        DataFrameWriter writer = readFourBinaryFilesAndWrite()
            .option(Options.WRITE_URI_REPLACE, ".*/src/test,/test")
            .mode(SaveMode.Append);

        SparkException ex = assertThrows(SparkException.class, writer::save);
        assertTrue(ex.getCause() instanceof ConnectorException);
        assertEquals("Each URI replacement value must be surrounded with single quotes: .*/src/test,/test",
            ex.getCause().getMessage());
    }

    @Test
    void uriTemplate() {
        Dataset<Row> dataset = newSparkSession()
            .read()
            .format("binaryFile")
            .load("src/test/resources/mixed-files/*.xml");

        Row row = dataset.collectAsList().get(0);
        // For as-yet unknown reasons, the timestamp in an InternalRow - which the connector writer receives - will
        // have 000 appended to it, thus capturing microseconds. But a Row will have the same value that the JVM
        // returns when getting lastModified for a File. This does not seem like an issue for a user, we just need to
        // account for it in our test.
        final String expectedURI = String.format("/testfile/%d000/%d.xml", row.getTimestamp(1).getTime(), row.getLong(2));

        defaultWrite(dataset.write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "template-test")
            .option(Options.WRITE_URI_TEMPLATE, "/testfile/{modificationTime}/{length}.xml")
        );

        XmlNode doc = readXmlDocument(expectedURI);
        doc.assertElementValue("/hello", "world");
    }

    @Test
    @Deprecated
    void forceDocumentType() {
        newSparkSession()
            .read()
            .format("binaryFile")
            .load("src/test/resources/json-unrecognized-extension/")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .options(defaultWriteOptions())
            // Verifies that the value gets capitalized.
            .option(Options.WRITE_FILE_ROWS_DOCUMENT_TYPE, "jSoN")
            .option(Options.WRITE_COLLECTIONS, "json-unrecognized-extension")
            .mode(SaveMode.Append)
            .save();

        JSONDocumentManager mgr = getDatabaseClient().newJSONDocumentManager();
        for (String uri : getUrisInCollection("json-unrecognized-extension", 2)) {
            JacksonHandle handle = mgr.read(uri, new JacksonHandle());
            assertEquals(Format.JSON, handle.getFormat());
            assertEquals("world", handle.get().get("hello").asText());
        }
    }

    @Test
    @Deprecated
    void invalidDocumentType() {
        DataFrameWriter writer = newSparkSession()
            .read()
            .format("binaryFile")
            .load("src/test/resources/json-unrecognized-extension/")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_FILE_ROWS_DOCUMENT_TYPE, "not valid")
            .mode(SaveMode.Append);

        SparkException ex = assertThrows(SparkException.class, writer::save);
        assertTrue(ex.getCause() instanceof ConnectorException);
        ConnectorException ce = (ConnectorException) ex.getCause();
        assertEquals("Invalid value for " + Options.WRITE_FILE_ROWS_DOCUMENT_TYPE + ": not valid; " +
            "must be one of 'JSON', 'XML', or 'TEXT'.", ce.getMessage());
    }

    private DataFrameWriter<Row> readFourBinaryFilesAndWrite() {
        return newSparkSession()
            .read()
            .format("binaryFile")
            .load("src/test/resources/mixed-files/")
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS);
    }
}
