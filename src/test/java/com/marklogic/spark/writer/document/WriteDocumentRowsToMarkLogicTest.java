/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.namespace.QName;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteDocumentRowsToMarkLogicTest extends AbstractIntegrationTest {

    @BeforeEach
    void setup() {
        TestUtil.insertTwoDocumentsWithAllMetadata(getDatabaseClient());
    }

    @Test
    void retainMetadataFromCopiedDocuments() {
        readTheTwoTestDocuments()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_TEMPLATE, "/new{URI}")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("collection1", 4);
        assertCollectionSize("collection2", 4);

        Stream.of("/new/test/1.xml", "/new/test/2.xml").forEach(uri -> {
            XmlNode doc = readXmlDocument(uri);
            doc.assertElementValue("/hello", "world");

            DocumentMetadataHandle metadata = readMetadata(uri);
            verifyCollections(metadata, "collection1", "collection2");
            verifyPropertiesAndMetadataValuesWereCopied(metadata);

            PermissionsTester tester = new PermissionsTester(metadata.getPermissions());
            assertEquals(2, tester.getDocumentPermissions().size());
            tester.assertReadPermissionExists("spark-user-role");
            tester.assertUpdatePermissionExists("spark-user-role");
            tester.assertReadPermissionExists("qconsole-user");
            assertEquals(2, tester.getDocumentPermissions().get("spark-user-role").size());
            assertEquals(1, tester.getDocumentPermissions().get("qconsole-user").size());
        });
    }

    /**
     * This ensures that the default suffix of ".json" isn't applied when the incoming row is a "document row" and
     * thus has an initial URI.
     */
    @Test
    void uriPrefix() {
        readTheTwoTestDocuments()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_PREFIX, "/backup")
            .option(Options.WRITE_COLLECTIONS, "backup-docs")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("backup-docs", 2);
        assertInCollections("/backup/test/1.xml", "backup-docs");
        assertInCollections("/backup/test/2.xml", "backup-docs");
    }

    @Test
    void writeXmlAsBinary() {
        readTheTwoTestDocuments()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_SUFFIX, ".unknown")
            .mode(SaveMode.Append)
            .save();

        Stream.of("/test/1.xml.unknown", "/test/2.xml.unknown").forEach(uri -> {
            String kind = getDatabaseClient().newServerEval()
                .xquery(String.format("xdmp:node-kind(fn:doc('%s')/node())", uri))
                .evalAs(String.class);
            assertEquals("binary", kind, "Verifying that MarkLogic stores the document as binary since it does not " +
                "recognize the 'unknown' extension.");
        });
    }

    @Test
    void forceDocumentType() {
        readTheTwoTestDocuments()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_SUFFIX, ".unknown")
            .option(Options.WRITE_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        Stream.of("/test/1.xml.unknown", "/test/2.xml.unknown").forEach(uri -> {
            String kind = getDatabaseClient().newServerEval()
                .xquery(String.format("xdmp:node-kind(fn:doc('%s')/node())", uri))
                .evalAs(String.class);
            assertEquals("element", kind, "MarkLogic should write each document as XML, as it doesn't recognize the " +
                "URI extension but the WRITE_DOCUMENT_TYPE option should force the document type in that scenario.");
        });
    }

    @Test
    void invalidDocumentType() {
        DataFrameWriter writer = readTheTwoTestDocuments()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_SUFFIX, ".unknown")
            .option(Options.WRITE_DOCUMENT_TYPE, "notvalid")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("Invalid value for spark.marklogic.write.documentType: notvalid; must be one of 'JSON', 'XML', or 'TEXT'.",
            ex.getMessage());
    }

    @Test
    void overrideMetadataFromCopiedDocuments() {
        readTheTwoTestDocuments()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_TEMPLATE, "/override{URI}")
            // Hmm... could we do metadata values like this:
            // .option(Options.WRITE_METADATA_VALUES, "meta1,value1,meta2,value2")
            // .option(Options.WRITE_DOCUMENT_PROPERTIES, "{org:example}key1,value1")
            .option(Options.WRITE_COLLECTIONS, "new1,new2")
            .option(Options.WRITE_PERMISSIONS, "spark-user-role,read,qconsole-user,update")
            .mode(SaveMode.Append)
            .save();

        // Make sure new docs weren't added to the test collections.
        assertCollectionSize("collection1", 2);
        assertCollectionSize("collection2", 2);

        Stream.of("/override/test/1.xml", "/override/test/2.xml").forEach(uri -> {
            XmlNode doc = readXmlDocument(uri);
            doc.assertElementValue("/hello", "world");

            DocumentMetadataHandle metadata = readMetadata(uri);
            verifyCollections(metadata, "new1", "new2");
            verifyPropertiesAndMetadataValuesWereCopied(metadata);

            PermissionsTester tester = new PermissionsTester(metadata.getPermissions());
            assertEquals(2, tester.getDocumentPermissions().size());
            tester.assertReadPermissionExists("spark-user-role");
            tester.assertUpdatePermissionExists("qconsole-user");
            assertEquals(1, tester.getDocumentPermissions().get("spark-user-role").size());
            assertEquals(1, tester.getDocumentPermissions().get("qconsole-user").size());
        });
    }

    private Dataset<Row> readTheTwoTestDocuments() {
        return newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "collection1")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load();
    }

    private void verifyCollections(DocumentMetadataHandle metadata, String... collections) {
        assertEquals(collections.length, metadata.getCollections().size());
        for (String c : collections) {
            assertTrue(metadata.getCollections().contains(c),
                String.format("Did not find collection %s in %s", c, metadata.getCollections()));
        }
    }

    private void verifyPropertiesAndMetadataValuesWereCopied(DocumentMetadataHandle metadata) {
        assertEquals(2, metadata.getProperties().size());
        assertEquals("value1", metadata.getProperties().get(QName.valueOf("{org:example}key1"), String.class));
        assertEquals("value2", metadata.getProperties().get(QName.valueOf("key2"), String.class));

        assertEquals(2, metadata.getMetadataValues().size());
        assertEquals("value1", metadata.getMetadataValues().get("meta1"));
        assertEquals("value2", metadata.getMetadataValues().get("meta2"));
    }
}
