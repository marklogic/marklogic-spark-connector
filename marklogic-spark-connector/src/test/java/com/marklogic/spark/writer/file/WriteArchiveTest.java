/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.jdom2.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WriteArchiveTest extends AbstractIntegrationTest {

    @BeforeEach
    void beforeEach() {
        TestUtil.insertTwoDocumentsWithAllMetadata(getDatabaseClient());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "metadata",
        "permissions",
        "collections",
        "quality",
        "properties",
        "metadatavalues"
    })
    void writeAllMetadata(String metadata, @TempDir Path tempDir) {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content," + metadata)
            .load()
            .repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length, "Expecting 1 zip since repartition created 1 partition writer.");
        verifyMetadataFiles(tempDir, metadata);
    }

    @Test
    void streaming(@TempDir Path tempDir) {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.STREAM_FILES, true)
            .load();

        dataset.collectAsList().forEach(row -> {
            assertNotNull(row.getString(0), "The URI column should have the URI of the document to retrieve during the writer phase.");
            for (int i = 1; i < DocumentRowSchema.SCHEMA.size(); i++) {
                assertTrue(row.isNullAt(i), "Every other column in the row should be null. We don't want the content, " +
                    "as that will be retrieved by the writer. And we unfortunately can't get the metadata without " +
                    "getting the content as well via a POST to v1/documents. So the writer will get the metadata " +
                    "as well.");
            }
        });

        dataset.repartition(1)
            .write()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,collections")
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.STREAM_FILES, true)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        verifyMetadataFiles(tempDir, "collections");
    }

    private void verifyMetadataFiles(Path tempDir, String metadataValue) {
        final List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();

        assertEquals(4, rows.size(), "Expecting the 2 author JSON entries and 2 entries for metadata.");

        for (int i = 0; i < 4; i += 2) {
            String entryName = rows.get(i).getString(0);
            assertTrue(entryName.endsWith(".metadata"), "The metadata should come before the content entry. " +
                "This allows for the content to later be streamed back into MarkLogic. Entry name: " + entryName);
        }

        final String expectedUriPrefix = "file:" + tempDir.toFile().getAbsolutePath();
        for (Row row : rows) {
            String uri = row.getString(0);
            assertTrue(uri.startsWith(expectedUriPrefix), "Unexpected URI, which is expected to start with the " +
                "absolute path of the zip file: " + uri);

            if (uri.endsWith(".xml")) {
                XmlNode doc = new XmlNode(new String((byte[]) row.get(1)));
                doc.assertElementValue("/hello", "world");
            } else {
                assertTrue(uri.endsWith(".metadata"));
                verifyMetadata(row, metadataValue);
            }
        }
    }

    private void verifyMetadata(Row row, String metadataValue) {
        String xml = new String((byte[]) row.get(1));
        XmlNode metadata = new XmlNode(xml,
            Namespace.getNamespace("rapi", "http://marklogic.com/rest-api"),
            PROPERTIES_NAMESPACE,
            Namespace.getNamespace("ex", "org:example"));

        switch (metadataValue) {
            case "collections":
                verifyCollections(metadata);
                verifyPermissionsMissing(metadata);
                break;
            case "permissions":
                verifyPermissions(metadata);
                break;
            case "quality":
                verifyQuality(metadata);
                verifyPermissionsMissing(metadata);
                break;
            case "properties":
                verifyProperties(metadata);
                verifyPermissionsMissing(metadata);
                break;
            case "metadatavalues":
                verifyMetadataValues(metadata);
                verifyPermissionsMissing(metadata);
                break;
            case "metadata":
                verifyCollections(metadata);
                verifyPermissions(metadata);
                verifyQuality(metadata);
                verifyProperties(metadata);
                verifyMetadataValues(metadata);
                break;
        }
    }

    private void verifyCollections(XmlNode metadata) {
        metadata.assertElementValue("/rapi:metadata/rapi:collections/rapi:collection", "collection1");
        metadata.assertElementValue("/rapi:metadata/rapi:collections/rapi:collection", "collection2");
    }

    private void verifyPermissions(XmlNode metadata) {
        String path = "/rapi:metadata/rapi:permissions/rapi:permission";
        metadata.assertElementExists(path + "[rapi:role-name = 'spark-user-role' and rapi:capability='update']");
        metadata.assertElementExists(path + "[rapi:role-name = 'spark-user-role' and rapi:capability='read']");
        metadata.assertElementExists(path + "[rapi:role-name = 'qconsole-user' and rapi:capability='read']");
    }

    private void verifyPermissionsMissing(XmlNode metadata) {
        metadata.assertElementMissing("Permissions should not exist since they were not in the set of " +
            "metadata categories.", "/rapi:metadata/rapi:permissions");
    }

    private void verifyQuality(XmlNode metadata) {
        metadata.assertElementValue("/rapi:metadata/rapi:quality", "10");
    }

    private void verifyProperties(XmlNode metadata) {
        metadata.assertElementValue("/rapi:metadata/prop:properties/ex:key1", "value1");
        metadata.assertElementValue("/rapi:metadata/prop:properties/key2", "value2");
    }

    private void verifyMetadataValues(XmlNode metadata) {
        metadata.prettyPrint();
    }
}
