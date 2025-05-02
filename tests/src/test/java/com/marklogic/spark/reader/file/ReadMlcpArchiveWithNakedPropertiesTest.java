/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A "naked properties" URI in MarkLogic is possible by creating a properties fragment at a URI but not
 * assigning any document content to it. MLCP archives can contain these, and thus we need to support them when reading
 * an MLCP archive. However, because v1/search cannot find these documents, it's not possible for the archives created
 * by this connector to contain them.
 */
class ReadMlcpArchiveWithNakedPropertiesTest extends AbstractIntegrationTest {

    private static final int PROPERTIES_COLUMN = 6;

    /**
     * The plumbing in the parent class for deleting documents before a test runs won't catch naked properties created
     * by this test, so we ensure they're deleted here.
     */
    @BeforeEach
    void deleteNakedPropertiesFromPreviousTestRuns() {
        Stream.of("example.xml.naked", "example2.xml.naked", "naked/example.xml.naked").forEach(uri -> {
            String query = String.format("xdmp:document-delete('%s')", uri);
            try {
                getDatabaseClient().newServerEval().xquery(query).evalAs(String.class);
            } catch (Exception e) {
                logger.debug("Ignoring this error because it's only due to the naked properties fragment not existing");
            }
        });
    }

    @Test
    void twoNakedEntries() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load("src/test/resources/mlcp-archive-files/two-naked-entries.zip")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "naked")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Using v1/search should not find the naked URIs since they do not have a document " +
            "associated with them", "naked", 0);

        Stream.of("example.xml.naked", "example2.xml.naked").forEach(uri -> {
            String collection = getDatabaseClient().newServerEval()
                .javascript(String.format("xdmp.documentGetCollections('%s')[0]", uri))
                .evalAs(String.class);
            assertEquals("naked", collection, "Each naked properties document should still be assigned to the " +
                "collection found in its MLCP metadata entry from the archive file. But these URIs aren't returned " +
                "by v1/search since there are no documents associated with them.");
        });

        XmlNode props = readDocumentProperties("example.xml.naked");
        props.assertElementValue("/prop:properties/priority", "1");
        props = readDocumentProperties("example2.xml.naked");
        props.assertElementValue("/prop:properties/priority", "2");
    }

    @Test
    void normalAndNakedEntry() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_TYPE, "mlcp_archive")
            .load("src/test/resources/mlcp-archive-files/normal-and-naked-entry.zip");

        List<Row> rows = dataset.collectAsList();
        assertEquals(2, rows.size(), "The example.xml.naked entry should have produced 1 row.");
        assertEquals("xml/1.xml", rows.get(1).getString(0));

        final String expectedNakedPropertiesUrl = "naked/example.xml.naked";
        Row nakedRow = rows.get(0);
        assertEquals(expectedNakedPropertiesUrl, nakedRow.getString(0));
        assertTrue(nakedRow.isNullAt(1), "Content should be null.");
        assertTrue(nakedRow.isNullAt(2), "Format should be null, since there's no content.");
        XmlNode properties = new XmlNode(nakedRow.getString(PROPERTIES_COLUMN), PROPERTIES_NAMESPACE);
        properties.assertElementValue("/prop:properties/priority", "1");

        // Write the rows to verify that the naked document is created correctly.
        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "naked-test")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        List<String> uris = getUrisInCollection("naked-test", 1);
        assertEquals("xml/1.xml", uris.get(0), "getUrisInCollection uses v1/search to find URIs, and thus it " +
            "should only find the URI of the normal document and not the one of the naked properties document.");

        XmlNode nakedProperties = readDocumentProperties(expectedNakedPropertiesUrl);
        nakedProperties.assertElementValue(
            "As of Java Client 6.6.1, a DMSDK WriteBatcher now allows for a document to have a null content handle, " +
                "which allows for 'naked properties' URIs to be written.",
            "/prop:properties/priority", "1");
    }
}
