/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.junit5.PermissionsTester;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamArchiveFileTest extends AbstractIntegrationTest {

    @Test
    void streamArchive() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.STREAM_FILES, true)
            .load("src/test/resources/archive-files/new-format-archive.zip");

        dataset.collectAsList().forEach(row -> {
            assertNotNull(row.getString(0), "The URI column should not be empty");
            FileContext fileContext = deserializeContentColumn(row);
            assertNotNull(fileContext, "The FileContext should be serialized to the content column so that it " +
                "can be used during the writer phase to read each file.");

            // All metadata columns should be null, as we haven't read anything from the file yet.
            for (int i = 2; i < DocumentRowSchema.SCHEMA.size(); i++) {
                assertTrue(row.isNullAt(i));
            }
        });

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("collection1", 2);
        assertCollectionSize("collection2", 2);
        readXmlDocument("test/1.xml").assertElementValue("/hello", "world");
        readXmlDocument("test/2.xml").assertElementValue("/hello", "world");
    }

    @Test
    void archiveCategories() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.READ_ARCHIVES_CATEGORIES, "permissions")
            .option(Options.STREAM_FILES, true)
            .load("src/test/resources/archive-files/new-format-archive.zip")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .mode(SaveMode.Append)
            .save();

        String message = "The two docs should not in the collections defined in the archive since the categories " +
            "option only specified permissions.";
        assertCollectionSize(message, "collection1", 0);
        assertCollectionSize(message, "collection2", 0);

        Stream.of("test/1.xml", "test/2.xml").forEach(uri -> {
            readXmlDocument(uri).assertElementValue("/hello", "world");

            // Verify permissions were read from the archive file.
            PermissionsTester permissions = readDocumentPermissions("test/1.xml");
            permissions.assertReadPermissionExists("spark-user-role");
            permissions.assertUpdatePermissionExists("spark-user-role");
            permissions.assertReadPermissionExists("qconsole-user");
        });
    }

    @Test
    void spacesInEntryNames() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.STREAM_FILES, true)
            .load("src/test/resources/archive-files/new-format-with-spaces.zip")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("collection1", 2);
        assertCollectionSize("collection2", 2);

        String message = "The URI unfortunately needs to have spaces encoded so that the PUT endpoint will accept " +
            "- see MLE-17088 for details. And the PUT endpoint must be used in order to stream each document from " +
            "the zip into MarkLogic.";
        readXmlDocument("test/first%20file.xml").assertElementValue(message, "/hello", "world");
        readXmlDocument("test/second%20file.xml").assertElementValue(message, "/hello", "world");
    }

    /**
     * Verifies that 2 or more archives are correctly streamed.
     */
    @Test
    void multipleArchives() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "archive")
            .option(Options.STREAM_FILES, true)
            .load(
                "src/test/resources/archive-files/new-format-archive.zip",
                "src/test/resources/archive-files/new-format-with-spaces.zip"
            )
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("collection1", 4);
        assertCollectionSize("collection2", 4);
        Stream.of("test/1.xml", "test/2.xml", "test/first%20file.xml", "test/second%20file.xml").forEach(uri ->
            readXmlDocument(uri).assertElementValue("/hello", "world")
        );
    }

    private FileContext deserializeContentColumn(Row row) {
        byte[] bytes = (byte[]) row.get(1);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try {
            return (FileContext) new ObjectInputStream(bais).readObject();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
