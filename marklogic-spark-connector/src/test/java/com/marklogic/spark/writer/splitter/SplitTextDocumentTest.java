/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
        doc.assertElementValue("/model:root/model:source-uri", "/test/marklogic-docs/java-client-intro.txt");
        doc.assertElementCount("Expecting 2 chunks based on the default max chunk size of 1000",
            "/model:root/model:chunks/model:chunk", 2);

        PermissionsTester tester = readDocumentPermissions(chunksUri);
        tester.assertUpdatePermissionExists("This is just a temporary permission until we allow the URI and " +
            "metadata for chunk documents to be configurable", "spark-user-role");
        tester.assertReadPermissionExists("spark-user-role");
    }

    @Test
    void inputDocumentHasUnknownFormat() {
        Dataset<Row> dataset = newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/mixed-files/hello.txt");

        assertTrue(dataset.collectAsList().get(0).isNullAt(2),
            "The connector is not expected to determine document type when reading files.");

        dataset.write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_URI_TEMPLATE, "/test/hello.txt")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/test/hello.txt-chunks-1.json");
        assertEquals("hello world", doc.get("chunks").get(0).get("text").asText(),
            "When the input document format is UNKNOWN and max chunks is zero, the connector should realize it " +
                "cannot add chunks to a document with format=UNKNOWN and thus it should create a separate chunks " +
                "document containing all the chunks.");
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
        firstChunkDoc.assertElementValue("/model:root/model:source-uri", "/test/marklogic-docs/java-client-intro.txt");
        firstChunkDoc.assertElementCount("/model:root/model:chunks/model:chunk", 3);

        XmlNode secondChunkDoc = readXmlDocument("/test/marklogic-docs/java-client-intro.txt-chunks-2.xml");
        secondChunkDoc.assertElementValue("/model:root/model:source-uri", "/test/marklogic-docs/java-client-intro.txt");
        secondChunkDoc.assertElementCount("/model:root/model:chunks/model:chunk", 1);
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
            doc.assertElementValue("/model:root/model:source-uri", "/test/marklogic-docs/java-client-intro.txt");
            doc.assertElementCount("/model:root/model:chunks/model:chunk", 2);
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
