/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.Row;
import org.jdom2.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReadDocumentRowsWithMetadataTest extends AbstractIntegrationTest {

    @BeforeEach
    void setup() {
        TestUtil.insertTwoDocumentsWithAllMetadata(getDatabaseClient());
    }

    @Test
    void contentAndAllMetadata() {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .collectAsList()
            .forEach(row -> {
                verifyUriColumn(row);
                verifyContentAndFormatColumnsArePopulated(row);
                verifyAllMetadataColumnsArePopulated(row);
            });
    }

    @Test
    void contentAndEachMetadataType() {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,collections,permissions,quality,properties,metadatavalues")
            .load()
            .collectAsList()
            .forEach(row -> {
                verifyUriColumn(row);
                verifyContentAndFormatColumnsArePopulated(row);
                verifyAllMetadataColumnsArePopulated(row);
            });
    }

    @Test
    void noMetadata() {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .load()
            .collectAsList()
            .forEach(row -> {
                verifyUriColumn(row);
                verifyContentAndFormatColumnsArePopulated(row);
                for (int i = 3; i <= 7; i++) {
                    assertNull(row.get(i), "Expected column " + i + " to be null because no metadata was requested.");
                }
            });
    }

    @Test
    void allMetadataAndNoContent() {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "metadata")
            .load()
            .collectAsList()
            .forEach(row -> {
                verifyUriColumn(row);
                verifyAllMetadataColumnsArePopulated(row);
                assertNull(row.get(1), "The content column should be empty since only metadata was requested.");
                assertNull(row.get(2), "The format column should be empty since only metadata was requested.");
            });
    }

    @Test
    void someMetadataAndNoContent() {
        newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "collections,permissions")
            .load()
            .collectAsList()
            .forEach(row -> {
                verifyUriColumn(row);
                verifyCollectionsColumn(row);
                verifyPermissionsColumn(row);
                assertNull(row.get(1));
                assertNull(row.get(2));
                assertNull(row.get(5));
                assertNull(row.get(6));
                assertNull(row.get(7));
            });
    }

    private void verifyUriColumn(Row row) {
        String uri = row.getString(0);
        assertTrue(uri.startsWith("/test/"), "Unexpected URI: " + uri);
    }

    private void verifyContentAndFormatColumnsArePopulated(Row row) {
        assertNotNull(row.get(1), "Content column should not be null");
        assertEquals("XML", row.getString(2));
    }

    private void verifyAllMetadataColumnsArePopulated(Row row) {
        verifyCollectionsColumn(row);
        verifyPermissionsColumn(row);

        assertEquals(10, row.getInt(5));

        XmlNode properties = new XmlNode(row.getString(6), Namespace.getNamespace("ex", "org:example"),
            PROPERTIES_NAMESPACE);
        properties.assertElementValue("/prop:properties/ex:key1", "value1");
        properties.assertElementValue("/prop:properties/key2", "value2");

        Map<String, String> metadataValues = JavaConverters.mapAsJavaMap((scala.collection.immutable.Map) row.get(7));
        assertEquals(2, metadataValues.size());
        assertEquals("value1", metadataValues.get("meta1"));
        assertEquals("value2", metadataValues.get("meta2"));
    }

    private void verifyCollectionsColumn(Row row) {
        WrappedArray collections = (WrappedArray) row.get(3);
        assertEquals("collection1", collections.apply(0));
        assertEquals("collection2", collections.apply(1));
    }

    private void verifyPermissionsColumn(Row row) {
        Map<String, WrappedArray> permissions = JavaConverters.mapAsJavaMap((scala.collection.immutable.Map) row.get(4));
        assertEquals(2, permissions.size());
        assertTrue(permissions.containsKey("spark-user-role"));
        assertTrue(permissions.containsKey("qconsole-user"));

        WrappedArray capabilities = permissions.get("spark-user-role");
        assertEquals(2, capabilities.length());
        assertTrue(capabilities.contains("READ"));
        assertTrue(capabilities.contains("UPDATE"));
        capabilities = permissions.get("qconsole-user");
        assertEquals(1, capabilities.length());
        assertTrue(capabilities.contains("READ"));
    }
}
