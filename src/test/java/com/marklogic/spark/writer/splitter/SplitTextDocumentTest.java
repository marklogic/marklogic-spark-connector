/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.jdom2.Namespace;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SplitTextDocumentTest extends AbstractIntegrationTest {

    @Test
    void jsonChunks() {
        prepareToWriteChunkDocuments()
            .mode(SaveMode.Append)
            .save();

        final String chunksUri = "/test/marklogic-docs/java-client-intro.txt-chunks-1.json";

        JsonNode doc = readJsonDocument(chunksUri);
        assertEquals("/test/marklogic-docs/java-client-intro.txt", doc.get("source-uri").asText());
        assertEquals(2, doc.get("chunks").size());

        PermissionsTester tester = readDocumentPermissions(chunksUri);
        tester.assertUpdatePermissionExists("This is just a temporary permission until we allow the URI and " +
            "metadata for chunk documents to be configurable", "spark-user-role");
        tester.assertReadPermissionExists("spark-user-role");
    }

    @Test
    void xmlChunks() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        final String chunksUri = "/test/marklogic-docs/java-client-intro.txt-chunks-1.xml";

        XmlNode doc = readXmlDocument(chunksUri);
        doc.assertElementValue("/root/source-uri", "/test/marklogic-docs/java-client-intro.txt");
        doc.assertElementCount("Expecting 2 chunks based on the default max chunk size of 1000",
            "/root/chunks/chunk", 2);

        PermissionsTester tester = readDocumentPermissions(chunksUri);
        tester.assertUpdatePermissionExists("This is just a temporary permission until we allow the URI and " +
            "metadata for chunk documents to be configurable", "spark-user-role");
        tester.assertReadPermissionExists("spark-user-role");
    }

    @Test
    void maxChunksOfThree() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 3)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .option(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        assertCollectionSize("Two chunk documents should have been written, with the first having 3 chunks and " +
            "the second having 1 chunk.", "chunks", 2);

        XmlNode firstChunkDoc = readXmlDocument("/test/marklogic-docs/java-client-intro.txt-chunks-1.xml");
        firstChunkDoc.assertElementValue("/root/source-uri", "/test/marklogic-docs/java-client-intro.txt");
        firstChunkDoc.assertElementCount("/root/chunks/chunk", 3);

        XmlNode secondChunkDoc = readXmlDocument("/test/marklogic-docs/java-client-intro.txt-chunks-2.xml");
        secondChunkDoc.assertElementValue("/root/source-uri", "/test/marklogic-docs/java-client-intro.txt");
        secondChunkDoc.assertElementCount("/root/chunks/chunk", 1);
    }

    @Test
    void maxChunksWithCustomPermissions() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE, "xml")
            .option(Options.WRITE_SPLITTER_SIDECAR_PERMISSIONS,
                "spark-user-role,read,spark-user-role,update,qconsole-user,read")
            .mode(SaveMode.Append)
            .save();

        PermissionsTester tester = readDocumentPermissions("/test/marklogic-docs/java-client-intro.txt-chunks-1.xml");
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("qconsole-user");
    }

    @Test
    void maxChunksWithCustomUri() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .option(Options.WRITE_SPLITTER_SIDECAR_URI_PREFIX, "/chunk/")
            .option(Options.WRITE_SPLITTER_SIDECAR_URI_SUFFIX, ".xml")
            .option(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        getUrisInCollection("chunks", 2).forEach(uri -> {
            assertTrue(uri.startsWith("/chunk/"), "Unexpected URI: " + uri);
            assertTrue(uri.endsWith(".xml"), "Unexpected URI: " + uri);
            XmlNode doc = readXmlDocument(uri);
            doc.assertElementValue("/root/source-uri", "/test/marklogic-docs/java-client-intro.txt");
            doc.assertElementCount("/root/chunks/chunk", 2);
        });
    }

    @Test
    void maxChunksWithCustomRootNameAndNamespace() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 4)
            .option(Options.WRITE_SPLITTER_SIDECAR_ROOT_NAME, "sidecar")
            .option(Options.WRITE_SPLITTER_SIDECAR_XML_NAMESPACE, "org:example")
            .option(Options.WRITE_SPLITTER_SIDECAR_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/test/marklogic-docs/java-client-intro.txt-chunks-1.xml");
        doc.setNamespaces(new Namespace[]{Namespace.getNamespace("ex", "org:example")});
        doc.assertElementExists("/ex:sidecar");
        doc.assertElementValue("/ex:sidecar/ex:source-uri", "/test/marklogic-docs/java-client-intro.txt");
        doc.assertElementCount("/ex:sidecar/ex:chunks/ex:chunk", 4);
    }

    private DataFrameWriter prepareToWriteChunkDocuments() {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/marklogic-docs/java-client-intro.txt")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_URI_PREFIX, "/test");
    }
}