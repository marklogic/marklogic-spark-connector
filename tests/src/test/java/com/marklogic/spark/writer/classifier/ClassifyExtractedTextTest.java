/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.classifier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.core.classifier.TextClassifierFactory;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClassifyExtractedTextTest extends AbstractIntegrationTest {

    @Test
    void extractedJson() {
        startReadAndWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(1))
            .mode(SaveMode.Append)
            .save();

        verifyExtractedJsonDocumentHasMetadataAndClassification();
    }

    @Test
    void extractedXml() {
        startReadAndWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(1))
            .option(Options.WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE, "xml")
            .mode(SaveMode.Append)
            .save();

        verifyExtractedXmlDocumentHasMetadataAndClassification();
    }

    @Test
    void extractedJsonWithChunks() {
        startReadAndWrite()
            // 29 = 1 for the doc; 27 for the chunks; and then an extra one thrown in to ensure it causes a warning
            // to be logged but no error to be thrown.
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(29))
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = verifyExtractedJsonDocumentHasMetadataAndClassification();
        assertTrue(doc.has("chunks"));
        ArrayNode chunks = (ArrayNode) doc.get("chunks");
        assertEquals(27, chunks.size(), "Expecting 27 chunks based on the default of 1000 chars");
        for (int i = 0; i < chunks.size(); i++) {
            JsonNode chunk = chunks.get(i);
            assertTrue(chunk.has("text"));
            assertTrue(chunk.has("classification"));
            assertTrue(chunk.get("classification").has("SYSTEM"));
        }
    }

    @Test
    void withBatchSize() {
        startReadAndWrite()
            // It's fine if each mock response has more articles than expected, the connector is expected to ignore
            // the extra ones.
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(28))
            .option(Options.WRITE_CLASSIFIER_BATCH_SIZE, 10)
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .mode(SaveMode.Append)
            .save();

        assertEquals(3, TextClassifierFactory.MockTextClassifier.getTimesInvoked(), "The mock classifier should " +
            "have been invoked 3 times - with 10, 10, and then 8 articles.");
    }

    @Test
    void extractedJsonWithSidecarChunks() {
        startReadAndWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(28))
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 1)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        verifyExtractedJsonDocumentHasMetadataAndClassification();

        for (String chunkUri : getUrisInCollection("chunks", 27)) {
            JsonNode doc = readJsonDocument(chunkUri);
            assertEquals(1, doc.get("chunks").size());
            JsonNode chunk = doc.get("chunks").get(0);
            verifyJsonHasClassification(chunk);
        }
    }

    @Test
    void extractedXmlWithChunks() {
        startReadAndWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(28))
            .option(Options.WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE, "xml")
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .mode(SaveMode.Append)
            .save();

        XmlNode doc = verifyExtractedXmlDocumentHasMetadataAndClassification();
        int expectedChunkCount = 27;
        doc.assertElementCount("/model:root/model:chunks/model:chunk[model:text and model:classification]", expectedChunkCount);
        doc.assertElementCount("/model:root/model:chunks/model:chunk[model:classification/model:SYSTEM/@name = 'DeterminedLanguage']", expectedChunkCount);
    }

    @Test
    void extractedXmlWithSidecarChunks() {
        startReadAndWrite()
            .option(ClassifierTestUtil.MOCK_RESPONSE_OPTION, ClassifierTestUtil.buildMockResponse(28))
            .option(Options.WRITE_EXTRACTED_TEXT_DOCUMENT_TYPE, "xml")
            .option(Options.WRITE_SPLITTER_TEXT, true)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 1)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "chunks")
            .mode(SaveMode.Append)
            .save();

        verifyExtractedXmlDocumentHasMetadataAndClassification();

        for (String chunkUri : getUrisInCollection("chunks", 27)) {
            XmlNode doc = readXmlDocument(chunkUri);
            doc.assertElementExists("/model:root/model:chunks/model:chunk/model:classification/model:SYSTEM[@name = 'DeterminedLanguage']");
            doc.assertElementExists("/model:root/model:chunks/model:chunk/model:classification/model:SYSTEM[@name = 'LanguageGuessed']");
        }
    }

    private DataFrameWriter<Row> startReadAndWrite() {
        return newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .load("src/test/resources/extraction-files/marklogic-reference-architecture.pdf")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            // Force use of a single batch by matching the document + its 27 expected chunks.
            .option(Options.WRITE_CLASSIFIER_BATCH_SIZE, 28)
            .option(Options.WRITE_URI_TEMPLATE, "/aaa/arch.pdf")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_EXTRACTED_TEXT, true);
    }

    private JsonNode verifyExtractedJsonDocumentHasMetadataAndClassification() {
        JsonNode doc = readJsonDocument("/aaa/arch.pdf/extracted-text.json");
        assertTrue(doc.has("metadata"));
        assertEquals("application/pdf", doc.get("metadata").get("Content-Type").asText());
        verifyJsonHasClassification(doc);
        return doc;
    }

    private void verifyJsonHasClassification(JsonNode node) {
        assertTrue(node.has("classification"));
        assertTrue(node.get("classification").has("SYSTEM"));
    }

    private XmlNode verifyExtractedXmlDocumentHasMetadataAndClassification() {
        XmlNode doc = readXmlDocument("/aaa/arch.pdf/extracted-text.xml");
        doc.assertElementValue("/model:root/model:metadata/model:Content-Type", "application/pdf");
        doc.assertElementExists("/model:root/model:classification/model:SYSTEM[@name = 'DeterminedLanguage']");
        doc.assertElementExists("/model:root/model:classification/model:SYSTEM[@name = 'LanguageGuessed']");
        return doc;
    }
}
