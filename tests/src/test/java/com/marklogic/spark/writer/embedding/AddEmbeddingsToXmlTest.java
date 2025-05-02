/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.row.RowManager;
import com.marklogic.client.row.RowRecord;
import com.marklogic.client.row.RowSet;
import com.marklogic.junit5.RequiresMarkLogic12;
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
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AddEmbeddingsToXmlTest extends AbstractIntegrationTest {

    private static final String TEST_EMBEDDING_FUNCTION_CLASS = "com.marklogic.spark.writer.embedding.MinilmEmbeddingModelFunction";

    @ExtendWith(RequiresMarkLogic12.class)
    @Test
    void embeddingsInChunkDocuments() {
        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XPATH, "/node()/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "xml-vector-chunks")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .mode(SaveMode.Append)
            .save();

        verifyEachChunkOnDocumentHasAnEmbedding("/split-test.xml-chunks-1.xml");
        verifyEachChunkOnDocumentHasAnEmbedding("/split-test.xml-chunks-2.xml");
        verifyEachChunkIsReturnedByAVectorQuery("xml_chunks");
    }

    @ExtendWith(RequiresMarkLogic12.class)
    @Test
    void embeddingsInSourceDocument() {
        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XPATH, "/node()/text/text()")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_COLLECTIONS, "xml-vector-chunks")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .mode(SaveMode.Append)
            .save();

        verifyEachChunkOnDocumentHasAnEmbedding("/split-test.xml");
        verifyEachChunkIsReturnedByAVectorQuery("xml_chunks");
    }

    @Test
    void sidecarWithNamespace() {
        readDocument("/marklogic-docs/namespaced-java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.XPATH_NAMESPACE_PREFIX + "ex", "org:example")
            .option(Options.WRITE_SPLITTER_XPATH, "/ex:root/ex:text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 4)
            .option(Options.WRITE_SPLITTER_SIDECAR_ROOT_NAME, "sidecar")
            .option(Options.WRITE_SPLITTER_SIDECAR_XML_NAMESPACE, "org:example")
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "namespaced-xml-vector-chunks")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml-chunks-1.xml");
        doc.assertElementCount("/ex:sidecar/ex:chunks/ex:chunk", 4);
        for (XmlNode chunk : doc.getXmlNodes("/ex:sidecar/ex:chunks/ex:chunk")) {
            chunk.assertElementExists("/ex:chunk/ex:text");
            chunk.assertElementExists("When a namespace is specified for the sidecar XML document, that should " +
                "override the default namespace for the embedding element.", "/ex:chunk/ex:embedding");
        }
    }

    /**
     * This test verifies that when the source document does not have a namespace but the sidecar document does,
     * the chunks still get embeddings because the connector doesn't need to use a ChunkSelector. That is due to the
     * connector knowing that the splitter will return instances of DocumentAndChunks, which means the embedder can
     * access the chunks without having to find them.
     */
    @ExtendWith(RequiresMarkLogic12.class)
    @Test
    void sidecarWithCustomNamespace() {
        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XPATH, "/node()/text/text()")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 4)
            .option(Options.WRITE_SPLITTER_SIDECAR_ROOT_NAME, "sidecar")
            .option(Options.WRITE_SPLITTER_SIDECAR_XML_NAMESPACE, "org:example")
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "namespaced-xml-vector-chunks")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_EMBEDDING_NAMESPACE, "http://marklogic.com/appservices/model")
            .mode(SaveMode.Append)
            .save();

        verifyChunksInNamespacedSidecar();
        verifyEachChunkIsReturnedByAVectorQuery("namespaced_xml_chunks");
    }

    @Test
    void sidecarWithCustomNamespaceAndCustomEmbeddingNamespace() {
        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XPATH, "/node()/text/text()")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 100)
            .option(Options.WRITE_SPLITTER_SIDECAR_XML_NAMESPACE, "org:example")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_EMBEDDING_NAMESPACE, "org:acme")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml-chunks-1.xml");
        doc.assertElementValue("/ex:root/ex:source-uri", "/split-test.xml");
        doc.assertElementExists("/ex:root/ex:chunks/ex:chunk[1]/ex:text");
        doc.assertElementExists(
            "When splitting and adding embeddings, the user can specify a namespace both for the sidecar document " +
                "and a separate namespace for the embedding.",
            "/ex:root/ex:chunks/ex:chunk[1]/acme:embedding"
        );
    }

    @Test
    void sidecarWithNoNamespace() {
        readDocument("/marklogic-docs/java-client-intro.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_XPATH, "/node()/text/text()")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 100)
            .option(Options.WRITE_SPLITTER_SIDECAR_XML_NAMESPACE, "")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml-chunks-1.xml");
        doc.assertElementValue("/root/source-uri", "/split-test.xml");
        doc.assertElementExists("/root/chunks/chunk[1]/text");
        doc.assertElementExists("Since a namespace is specified for the document - no namespace - it should be " +
            "applied to the embedding element too.", "/root/chunks/chunk[1]/embedding");
    }

    @Test
    void customChunks() {
        readDocument("/marklogic-docs/custom-chunks.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_CHUNKS_XPATH, "/envelope/my-chunks/my-chunk")
            .option(Options.WRITE_EMBEDDER_TEXT_XPATH, "my-text/text()")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementCount("Each of the 2 custom chunks should have an 'embedding' element.",
            "/envelope/my-chunks/my-chunk[my-text and model:embedding]", 2);
    }

    @Test
    void namespacedCustomChunks() {
        readDocument("/marklogic-docs/namespaced-custom-chunks.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.XPATH_NAMESPACE_PREFIX + "ex", "org:example")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_CHUNKS_XPATH, "/ex:envelope/ex:my-chunks/ex:my-chunk")
            .option(Options.WRITE_EMBEDDER_TEXT_XPATH, "ex:my-text/text()")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementCount("Each of the 2 custom chunks should have an 'embedding' element.",
            "/ex:envelope/ex:my-chunks/ex:my-chunk[ex:my-text and model:embedding]", 2);
    }

    @Test
    void namespacedCustomChunksWithCustomEmbedding() {
        readDocument("/marklogic-docs/namespaced-custom-chunks.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.XPATH_NAMESPACE_PREFIX + "ex", "org:example")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_CHUNKS_XPATH, "/ex:envelope/ex:my-chunks/ex:my-chunk")
            .option(Options.WRITE_EMBEDDER_TEXT_XPATH, "ex:my-text/text()")
            .option(Options.WRITE_EMBEDDER_EMBEDDING_NAME, "my-embedding")
            .option(Options.WRITE_EMBEDDER_EMBEDDING_NAMESPACE, "org:marklogic")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml",
            Namespace.getNamespace("ex", "org:example"), Namespace.getNamespace("ml", "org:marklogic"));

        doc.assertElementCount("Each of the 2 custom chunks should have a custom embedding element. " +
                "In the 2.5.0 release, both chunks and embeddings were added, " +
                "but only embeddings can have their element name / namespace changed. It's not clear why this " +
                "support was added, as a user can always use a REST transform to adjust the data structure. And if " +
                "an embedding element already exists, that won't cause a conflict as the connector embedding element " +
                "is not appended and thus will not overwrite the existing element. These " +
                "options may be deprecated and removed in the future.",
            "/ex:envelope/ex:my-chunks/ex:my-chunk[ex:my-text and ml:my-embedding]", 2);
    }

    @Test
    void customChunksNoPathGiven() {
        DataFrameWriter writer = readDocument("/marklogic-docs/custom-chunks.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(writer::save);
        assertEquals("To generate embeddings on documents, you must specify either spark.marklogic.write.embedder.chunks.jsonPointer or spark.marklogic.write.embedder.chunks.xpath to define the location of chunks in documents.",
            ex.getMessage(), "When generating embeddings without splitting the text - i.e. when a document is read " +
                "from MarkLogic and chunks already exist - the user must specify the location of the chunks so that " +
                "that connector knows what type of document to expect.");
    }

    @Test
    void customChunksPathDoesntReturnAnyChunks() {
        readDocument("/marklogic-docs/custom-chunks.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_CHUNKS_XPATH, "/no-chunks-here")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementMissing("If the custom path for chunks doesn't point to anything, that's fine - no error " +
            "will be thrown, the document will simply be written without any embeddings added to it.", "//embedding");
    }

    @Test
    void textXPathDoesntReturnText() {
        readDocument("/marklogic-docs/custom-chunks.xml")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.xml")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_CHUNKS_XPATH, "/envelope/my-chunks/my-chunk")
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = readXmlDocument("/split-test.xml");
        doc.assertElementMissing("In this test, the text XPath should be '/my-text', but it defaults to '/text' and " +
            "thus the text in each chunk cannot be found. The thought is that this should not throw an error; it " +
            "simply means that an embedding cannot be generated for the chunk.", "//embedding");
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }

    private void verifyEachChunkOnDocumentHasAnEmbedding(String uri) {
        XmlNode doc = readXmlDocument(uri);
        doc.getXmlNodes("/node()/chunks/chunk").forEach(chunk -> {
            chunk.assertElementExists("/chunk/text");
            chunk.assertElementExists("/chunk/model:embedding");
        });
    }

    private void verifyEachChunkIsReturnedByAVectorQuery(String viewName) {
        RowManager rowManager = getDatabaseClient().newRowManager();
        PlanBuilder op = rowManager.newPlanBuilder();
        RowSet<RowRecord> rows = rowManager.resultRows(
            op.fromView("example", viewName, "")
                .bind(op.as(
                    op.col("vector_test"),
                    op.vec.vector(op.col("embedding"))
                ))
        );

        int counter = 0;
        for (RowRecord row : rows) {
            assertEquals("xs:string", row.getDatatype("uri"));
            assertEquals("http://marklogic.com/vector#vector", row.getDatatype("embedding"));
            assertEquals("http://marklogic.com/vector#vector", row.getDatatype("vector_test"));
            counter++;
        }

        assertEquals(4, counter, "Each test is expected to produce 4 chunks based on the max chunk size of 500.");
    }

    private void verifyChunksInNamespacedSidecar() {
        XmlNode doc = readXmlDocument("/split-test.xml-chunks-1.xml");
        doc.assertElementCount("/ex:sidecar/ex:chunks/ex:chunk", 4);
        for (XmlNode chunk : doc.getXmlNodes("/ex:sidecar/ex:chunks/ex:chunk")) {
            chunk.assertElementExists("/ex:chunk/ex:text");
            chunk.assertElementExists("The embedding should default to the MarkLogic-specific namespace when not " +
                "specified by the user.", "/ex:chunk/model:embedding");
        }
    }
}
