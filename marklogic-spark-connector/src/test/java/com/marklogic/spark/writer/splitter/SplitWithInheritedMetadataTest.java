/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.TestUtil;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simulates a copy process - documents are read with their metadata and then written with sidecar chunk documents,
 * thus exercising the full write pipeline for inheriting collections and permissions.
 */
class SplitWithInheritedMetadataTest extends AbstractIntegrationTest {

    private static final String JSON_CHUNK_URI = "/copied/inherit/doc1.json-chunks-1.json";
    private static final String XML_CHUNK_URI = "/copied/inherit/doc1.xml-chunks-1.xml";

    @BeforeEach
    void insertSourceDocuments() {
        DocumentMetadataHandle metadata = TestUtil.withDefaultPermissions(new DocumentMetadataHandle());
        metadata.getCollections().addAll("source-c1", "shared");
        metadata.getPermissions().add("qconsole-user", DocumentMetadataHandle.Capability.READ);

        getDatabaseClient().newJSONDocumentManager().write("/inherit/doc1.json", metadata,
            new StringHandle("{\"text\": \"Hello world\"}").withFormat(Format.JSON));
        getDatabaseClient().newXMLDocumentManager().write("/inherit/doc1.xml", metadata,
            new StringHandle("<root><text>Hello world</text></root>").withFormat(Format.XML));
    }

    @Test
    void inheritCollectionsAndPermissions() {
        copyJsonDocument()
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_COLLECTIONS, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_PERMISSIONS, true)
            .save();

        assertEquals(Set.of("source-c1", "shared"), readMetadata(JSON_CHUNK_URI).getCollections());

        PermissionsTester tester = readDocumentPermissions(JSON_CHUNK_URI);
        assertEquals(2, tester.getDocumentPermissions().size(), "Expecting the source roles spark-user-role and qconsole-user");
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("qconsole-user");
        assertEquals(1, tester.getDocumentPermissions().get("qconsole-user").size());
    }

    @Test
    void inheritCollectionsOnly() {
        copyJsonDocument()
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_COLLECTIONS, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS, DEFAULT_PERMISSIONS)
            .save();

        assertEquals(Set.of("source-c1", "shared"), readMetadata(JSON_CHUNK_URI).getCollections());

        PermissionsTester tester = readDocumentPermissions(JSON_CHUNK_URI);
        assertEquals(1, tester.getDocumentPermissions().size(), "Permissions should not be inherited when only " +
            "collections are inherited, so only the explicit sidecar permissions should exist.");
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
    }

    @Test
    void unionWithExplicitSidecarMetadata() {
        copyJsonDocument()
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_COLLECTIONS, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_PERMISSIONS, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "shared,chunks")
            .option(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS, "qconsole-user,update")
            .save();

        assertEquals(Set.of("source-c1", "shared", "chunks"), readMetadata(JSON_CHUNK_URI).getCollections());

        PermissionsTester tester = readDocumentPermissions(JSON_CHUNK_URI);
        assertEquals(2, tester.getDocumentPermissions().size());
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("Inherited from the source", "qconsole-user");
        tester.assertUpdatePermissionExists("Added via the explicit sidecar permissions", "qconsole-user");
        assertEquals(2, tester.getDocumentPermissions().get("qconsole-user").size());
    }

    @Test
    void jobLevelMetadataIsInheritedViaSourceDocument() {
        copyJsonDocument()
            .option(Options.WRITE_COLLECTIONS, "job-collection")
            // 'rest-reader' is a job-only role; 'qconsole-user' is a source-only role.
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS + ",rest-reader,read")
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_COLLECTIONS, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_PERMISSIONS, true)
            .save();

        assertEquals(Set.of("job-collection"), readMetadata(JSON_CHUNK_URI).getCollections(),
            "Job-level collections replace the row's collections on the source document, and the sidecar should " +
                "inherit the collections as written to the source document.");

        PermissionsTester tester = readDocumentPermissions(JSON_CHUNK_URI);
        assertEquals(2, tester.getDocumentPermissions().size(), "Job-level permissions replace the row's " +
            "permissions on the source document, so the sidecar should have only spark-user-role and rest-reader.");
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("rest-reader");
        assertEquals(1, tester.getDocumentPermissions().get("rest-reader").size());
    }

    @Test
    void xmlSourceDocument() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/inherit/doc1.xml")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_PREFIX, "/copied")
            .option(Options.WRITE_SPLITTER_XPATH, "/root/text/text()")
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_COLLECTIONS, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_PERMISSIONS, true)
            .mode(SaveMode.Append)
            .save();

        assertEquals(Set.of("source-c1", "shared"), readMetadata(XML_CHUNK_URI).getCollections());

        PermissionsTester tester = readDocumentPermissions(XML_CHUNK_URI);
        assertEquals(2, tester.getDocumentPermissions().size());
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("qconsole-user");
        assertEquals(1, tester.getDocumentPermissions().get("qconsole-user").size());
    }

    @Test
    void extractedTextSourceDocument() {
        newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/hello-world.docx")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_REPLACE, ".*/extraction-files,'/extract-test'")
            .option(Options.WRITE_COLLECTIONS, "main-doc")
            // 'rest-reader' is only on the main doc; 'qconsole-user' is only on the extracted-text doc.
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS + ",rest-reader,read")
            .option(Options.WRITE_EXTRACTED_TEXT, true)
            .option(Options.WRITE_EXTRACTED_TEXT_COLLECTIONS, "extracted-doc")
            .option(Options.WRITE_EXTRACTED_TEXT_PERMISSIONS, DEFAULT_PERMISSIONS + ",qconsole-user,read")
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_COLLECTIONS, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_INHERIT_PERMISSIONS, true)
            .mode(SaveMode.Append)
            .save();

        final String chunkUri = "/extract-test/hello-world.docx-extracted-text.json-chunks-1.json";
        assertEquals(Set.of("extracted-doc"), readMetadata(chunkUri).getCollections(),
            "The sidecar's source is the extracted-text doc, so it should inherit only the extracted-text collections.");

        PermissionsTester tester = readDocumentPermissions(chunkUri);
        assertEquals(2, tester.getDocumentPermissions().size(), "The sidecar should have only the extracted-text " +
            "permissions; rest-reader from the main doc must not be added.");
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("qconsole-user");
        assertEquals(1, tester.getDocumentPermissions().get("qconsole-user").size());
    }

    @Test
    void noInheritanceByDefault() {
        copyJsonDocument()
            .option(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS, DEFAULT_PERMISSIONS)
            .save();

        assertTrue(readMetadata(JSON_CHUNK_URI).getCollections().isEmpty());

        PermissionsTester tester = readDocumentPermissions(JSON_CHUNK_URI);
        assertEquals(1, tester.getDocumentPermissions().size());
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
    }

    private DataFrameWriter<Row> copyJsonDocument() {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/inherit/doc1.json")
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_URI_PREFIX, "/copied")
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .mode(SaveMode.Append);
    }
}
