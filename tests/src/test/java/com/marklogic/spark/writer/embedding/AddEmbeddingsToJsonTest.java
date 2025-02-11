/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.row.RowManager;
import com.marklogic.client.row.RowRecord;
import com.marklogic.client.row.RowSet;
import com.marklogic.junit5.RequiresMarkLogic12;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;

class AddEmbeddingsToJsonTest extends AbstractIntegrationTest {

    private static final String TEST_EMBEDDING_FUNCTION_CLASS = "com.marklogic.spark.writer.embedding.MinilmEmbeddingModelFunction";

    /**
     * Tests the use case where a user wants to split the text into chunks and generate embeddings for each chunk, all
     * as part of one write process.
     */
    @ExtendWith(RequiresMarkLogic12.class)
    @Test
    void splitToSeparateDocumentsAndAddEmbeddings() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_SPLITTER_SIDECAR_COLLECTIONS, "json-vector-chunks")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .mode(SaveMode.Append)
            .save();

        verifyEachChunkOnDocumentHasAnEmbedding("/split-test.json-chunks-1.json");
        verifyEachChunkOnDocumentHasAnEmbedding("/split-test.json-chunks-2.json");
        verifyEachChunkIsReturnedByAVectorQuery();
    }

    @ExtendWith(RequiresMarkLogic12.class)
    @Test
    void splitToSameDocumentAndAddEmbeddings() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_COLLECTIONS, "json-vector-chunks")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .mode(SaveMode.Append)
            .save();

        verifyEachChunkOnDocumentHasAnEmbedding("/split-test.json");
        verifyEachChunkIsReturnedByAVectorQuery();
    }

    /**
     * Tests the use case where a user first loads test with the text split into chunks. Then later on, the user
     * decides to add embeddings to the chunks.
     */
    @ExtendWith(RequiresMarkLogic12.class)
    @Test
    @Disabled("Temporarily disabled, not yet supporting this in the new approach")
    void addEmbeddingsToExistingSplits() {
        // Add splits to the test doc first.
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .mode(SaveMode.Append)
            .save();

        // Now add embeddings to the existing chunks, which are all on one document.
        readDocument("/split-test.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_COLLECTIONS, "json-vector-chunks")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER, "/chunks")
            .mode(SaveMode.Append)
            .save();

        verifyEachChunkOnDocumentHasAnEmbedding("/split-test.json");
        verifyEachChunkIsReturnedByAVectorQuery();
    }

    @Test
    void passOptionsToEmbeddingModelFunction() {
        DataFrameWriter writer = readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_OPTION_PREFIX + "throwError", "true")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("Unable to instantiate class for creating an embedding model; class name: com.marklogic.spark.writer.embedding.MinilmEmbeddingModelFunction; " +
                "cause: Intentional error.", ex.getMessage(),
            "This test verifies that a custom option can be sent to the embedding model function class. In this " +
                "case, we expect our custom class to throw an error when it receives the 'throwError' option.");
    }

    @Test
    void invalidEmbeddingModelFunctionClass() {
        DataFrameWriter writer = readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, "not.valid")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("Unable to instantiate class for creating an embedding model; class name: not.valid; " +
                "cause: Could not load class not.valid",
            ex.getMessage());
    }

    @Test
    @Disabled("Will support existing chunks in next PR")
    void customPaths() {
        readDocument("/marklogic-docs/custom-chunks.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER, "/envelope/my-chunks")
            .option(Options.WRITE_EMBEDDER_TEXT_JSON_POINTER, "/wrapper/my-text")
            .option(Options.WRITE_EMBEDDER_EMBEDDING_NAME, "my-embedding")
            .option(Options.WRITE_URI_TEMPLATE, "/custom-path-test.json")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/custom-path-test.json");
        ArrayNode chunks = (ArrayNode) doc.get("envelope").get("my-chunks");
        assertEquals(2, chunks.size());
        chunks.forEach(chunk -> {
            assertTrue(chunk.has("my-embedding"), "In the 2.5.0 release, both chunks and embeddings were added, " +
                "but only embeddings can have their element name changed. It's not clear why this support was " +
                "added instead of using the chunks approach, which is to use a name of e.g. 'embedder-embedding' " +
                "instead. This option may be deprecated and removed in the future.");
            assertEquals(JsonNodeType.ARRAY, chunk.get("my-embedding").getNodeType());
        });
    }

    @Test
    void invalidCustomTextPointer() {
        readDocument("/marklogic-docs/custom-chunks.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER, "/envelope/my-chunks")
            .option(Options.WRITE_EMBEDDER_TEXT_JSON_POINTER, "/doesnt-point-to-anything")
            .option(Options.WRITE_EMBEDDER_EMBEDDING_NAME, "my-embedding")
            .option(Options.WRITE_URI_TEMPLATE, "/custom-path-test.json")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/custom-path-test.json");
        ArrayNode chunks = (ArrayNode) doc.get("envelope").get("my-chunks");
        assertEquals(2, chunks.size());
        chunks.forEach(chunk -> {
            assertFalse(chunk.has("my-embedding"), "No embedding should have been added since the text pointer did " +
                "not to any text. Not adding an embedding currently seems preferable versus throwing an error when " +
                "a chunk does not have any text.");
            assertTrue(chunk.has("wrapper"));
            assertEquals(1, chunk.size(), "The chunk is expected to only have the wrapper/text field that it " +
                "had when the document was loaded.");
        });
    }

    @Test
    @Disabled("Will support embedding without splitting in next PR")
    void chunksIsAnObjectInsteadOfAnArray() {
        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, TEST_EMBEDDING_FUNCTION_CLASS)
            .option(Options.WRITE_EMBEDDER_CHUNKS_JSON_POINTER, "")
            .option(Options.WRITE_EMBEDDER_TEXT_JSON_POINTER, "/text")
            .option(Options.WRITE_URI_TEMPLATE, "/custom-path-test.json")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/custom-path-test.json");
        assertTrue(doc.has("embedding"));
        assertEquals(JsonNodeType.ARRAY, doc.get("embedding").getNodeType());
    }

    @Test
    @Disabled("Will support batch size in next PR")
    void testBatchSize() {
        TestEmbeddingModel.reset();

        readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 300)
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, "com.marklogic.spark.writer.embedding.TestEmbeddingModel")
            .option(Options.WRITE_EMBEDDER_BATCH_SIZE, 3)
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertEquals(8, doc.get("chunks").size());

        assertEquals(3, TestEmbeddingModel.batchCounter, "Expecting 2 batches to be sent to the test " +
            "embedding model, given the batch size of 2 and 4 chunks being created.");
    }

    @Test
    @Disabled("Will support batch size in next PR")
    void batchSizeIsHigherThanChunkCount() {
        TestEmbeddingModel.reset();

        readDocument("/marklogic-docs/java-client-intro.json")
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_SPLITTER_MAX_CHUNK_SIZE, 500)
            .option(Options.WRITE_SPLITTER_SIDECAR_MAX_CHUNKS, 2)
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, "com.marklogic.spark.writer.embedding.TestEmbeddingModel")
            .option(Options.WRITE_EMBEDDER_BATCH_SIZE, 10)
            .mode(SaveMode.Append)
            .save();

        assertEquals(1, TestEmbeddingModel.batchCounter, "Verifies that a single batch is sent, as the batch size is " +
            "higher then the total number of chunks. Debug logging should show the count of pending source documents " +
            "when the job is about to finish and should indicate that embeddings are generated for each one.");
        assertEquals(4, TestEmbeddingModel.chunkCounter);
    }

    @Test
    void invalidBatchSize() {
        DataFrameWriter writer = readDocument("/marklogic-docs/java-client-intro.json")
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .option(Options.WRITE_EMBEDDER_MODEL_FUNCTION_CLASS_NAME, "com.marklogic.spark.writer.embedding.TestEmbeddingModel")
            .option(Options.WRITE_EMBEDDER_BATCH_SIZE, "abc")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(() -> writer.save());
        assertEquals("The value of 'spark.marklogic.write.embedder.batchSize' must be numeric.", ex.getMessage());
    }

    private Dataset<Row> readDocument(String uri) {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_CATEGORIES, "content,metadata")
            .option(Options.READ_DOCUMENTS_URIS, uri)
            .load();
    }

    private void verifyEachChunkOnDocumentHasAnEmbedding(String uri) {
        JsonNode doc = readJsonDocument(uri);
        ArrayNode chunks = (ArrayNode) doc.get("chunks");
        chunks.forEach(node -> {
            assertTrue(node.has("text"));
            assertTrue(node.has("embedding"));
            assertEquals(JsonNodeType.ARRAY, node.get("embedding").getNodeType());
        });
    }

    private void verifyEachChunkIsReturnedByAVectorQuery() {
        RowManager rowManager = getDatabaseClient().newRowManager();
        PlanBuilder op = rowManager.newPlanBuilder();
        RowSet<RowRecord> rows = rowManager.resultRows(
            op.fromView("example", "json_chunks", "")
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
}
