/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.jdom2.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private void verifyMetadataFiles(Path tempDir, String metadataValue) {
        final List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load(tempDir.toFile().getAbsolutePath())
            .collectAsList();

        assertEquals(4, rows.size(), "Expecting the 2 author JSON entries and 2 entries for metadata.");

        final String expectedUriPrefix = "file://" + tempDir.toFile().getAbsolutePath();
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
                break;
            case "permissions":
                verifyPermissions(metadata);
                break;
            case "quality":
                verifyQuality(metadata);
                break;
            case "properties":
                verifyProperties(metadata);
                break;
            case "metadatavalues":
                verifyMetadataValues(metadata);
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
