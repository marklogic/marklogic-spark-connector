/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.marklogic.junit5.PermissionsTester;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.jdom2.Namespace;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SplitXmlDocumentTest extends AbstractIntegrationTest {

    @Test
    void splitXmlDocument() {
        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/root/text/text()")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, 0)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementCount("Expecting 4 chunks based on a max chunk size of 500", "/root/chunks/chunk", 4);

        String firstChunk = doc.getElementValue("/root/chunks/chunk[1]/text");
        assertTrue(firstChunk.startsWith("When working with the Java API"), "The first chunk should begin with the " +
            "text in the original 'text' element. Actual chunk: " + firstChunk);
    }

    @Test
    void withNamespace() {
        readDocument("/marklogic-docs/namespaced-java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/ex:root/ex:text/text()")
            .option(Options.WRITE_SPLITTER_XML_NAMESPACE_PREFIX + "ex", "org:example")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/namespace-test.xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/namespace-test.xml");
        doc.setNamespaces(new Namespace[]{Namespace.getNamespace("ex", "org:example")});

        doc.assertElementCount("Expecting 2 chunks based on the default max chunk size of 1000. And the " +
                "chunks and chunk elements are expected to not be in a namespace. But the user's declaration of " +
                "the 'ex' prefix should have allowed the XPath statement for selecting text to succeed.",
            "/ex:root/chunks/chunk", 2);
    }

    @Test
    void undeclaredNamespace() {
        DataFrameWriter writer = readDocument("/marklogic-docs/namespaced-java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/ex:root/ex:text/text()")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("Unable to split document using XPath expression: /ex:root/ex:text/text(); cause: " +
                "Namespace with prefix 'ex' has not been declared.",
            ex.getMessage()
        );
    }

    @Test
    void overlapSizeGreaterThanChunkSize() {
        DataFrameWriter writer = readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/root/text/text()")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 200)
            .option(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, 300)
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("Unable to create splitter for documents; cause: spark.marklogic.write.splitter.maxOverlapSize " +
            "must be between 0 and 200, but is: 300", ex.getMessage().trim());
    }

    @Test
    void chunkSizeBelowZero() {
        DataFrameWriter writer = readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/root/text/text()")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, -1)
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("The value of 'spark.marklogic.write.splitter.maxChunkSize' must be 0 or greater.", ex.getMessage());
    }

    @Test
    void overlapSizeBelowZero() {
        DataFrameWriter writer = readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/root/text/text()")
            .option(Options.WRITE_SPLITTER_MAX_OVERLAP_SIZE, -1)
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("The value of 'spark.marklogic.write.splitter.maxOverlapSize' must be 0 or greater.", ex.getMessage());
    }

    @Test
    void regexSplitter() {
        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/root/text/text()")
            .option(Options.WRITE_SPLITTER_REGEX, "basic architecture")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementCount("The pattern 'basic architecture' appears one time in the <text> element, and so " +
            "2 chunks should be produced.", "/root/chunks/chunk", 2);

        String firstChunk = doc.getElementValue("/root/chunks/chunk[1]/text");
        assertTrue(firstChunk.endsWith("This chapter covers a number of"), "The first chunk should end with the " +
            "text that appears right before the one occurrence of 'basic architecture'. Actual chunk: " + firstChunk);
    }

    @Test
    void invalidRegex() {
        DataFrameWriter writer = readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/root/text/text()")
            .option(Options.WRITE_SPLITTER_REGEX, ".*(not valid")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertTrue(ex.getMessage().contains("Cannot split documents due to invalid regex: .*(not valid; cause: Unclosed group"),
            "Unexpected error message: " + ex.getMessage());
    }

    @Test
    void regexProducesTooLargeChunk() {
        DataFrameWriter writer = readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/root/text/text()")
            .option(Options.WRITE_SPLITTER_REGEX, "basic architecture")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 100)
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertTrue(ex.getMessage().contains("Unable to split document with URI: /marklogic-docs/java-client-intro.xml; cause: " +
                "The text \"When working with the Java API...\" (886 characters long) doesn't fit into the " +
                "maximum segment size (100 characters)"),
            "The underlying langchain4j splitter is expected to throw an error due to the regex producing a segment " +
                "that exceeds the max chunk size. Actual error: " + ex.getMessage());
    }

    @Test
    void maxChunksOfThree() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 3)
            .option(Options.WRITE_SPLITTER_OUTPUT_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementMissing("Chunks should have been written to separate documents since max chunks is " +
            "greater than zero.", "//chunks");

        assertCollectionSize("Two chunk documents should have been written, with the first having 3 chunks and " +
            "the second having 1 chunk.", "chunks", 2);

        XmlNode firstChunkDoc = readXmlDocument("/split-test.xml-chunks-0.xml");
        firstChunkDoc.assertElementValue("/root/source-uri", "/split-test.xml");
        firstChunkDoc.assertElementCount("/root/chunks/chunk", 3);

        XmlNode secondChunkDoc = readXmlDocument("/split-test.xml-chunks-1.xml");
        secondChunkDoc.assertElementValue("/root/source-uri", "/split-test.xml");
        secondChunkDoc.assertElementCount("/root/chunks/chunk", 1);
    }

    @Test
    void maxChunksWithCustomPermissions() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 1000)
            .option(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_OUTPUT_PERMISSIONS,
                "spark-user-role,read,spark-user-role,update,qconsole-user,read")
            .mode(SaveMode.Append)
            .save();

        PermissionsTester tester = readDocumentPermissions("/split-test.xml-chunks-0.xml");
        tester.assertReadPermissionExists("spark-user-role");
        tester.assertUpdatePermissionExists("spark-user-role");
        tester.assertReadPermissionExists("qconsole-user");
    }

    @Test
    void maxChunksWithCustomUri() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_OUTPUT_COLLECTIONS, "chunks")
            .option(Options.WRITE_SPLITTER_OUTPUT_URI_PREFIX, "/chunk/")
            .option(Options.WRITE_SPLITTER_OUTPUT_URI_SUFFIX, ".xml")
            .mode(SaveMode.Append)
            .save();

        getUrisInCollection("chunks", 2).forEach(uri -> {
            assertTrue(uri.startsWith("/chunk/"), "Unexpected URI: " + uri);
            assertTrue(uri.endsWith(".xml"), "Unexpected URI: " + uri);
            XmlNode doc = readXmlDocument(uri);
            doc.assertElementValue("/root/source-uri", "/split-test.xml");
            doc.assertElementCount("/root/chunks/chunk", 2);
        });
    }

    @Test
    void maxChunksWithCustomRootNameAndNamespace() {
        prepareToWriteChunkDocuments()
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_OUTPUT_MAX_CHUNKS, 4)
            .option(Options.WRITE_SPLITTER_OUTPUT_ROOT_NAME, "sidecar")
            .option(Options.WRITE_SPLITTER_OUTPUT_XML_NAMESPACE, "org:example")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml-chunks-0.xml");
        doc.setNamespaces(new Namespace[]{Namespace.getNamespace("ex", "org:example")});
        doc.assertElementExists("/ex:sidecar");
        doc.assertElementValue("/ex:sidecar/ex:source-uri", "/split-test.xml");
        doc.assertElementCount("/ex:sidecar/ex:chunks/ex:chunk", 4);
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }

    private DataFrameWriter prepareToWriteChunkDocuments() {
        return readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XML_PATH, "/root/text/text()")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml");
    }
}
