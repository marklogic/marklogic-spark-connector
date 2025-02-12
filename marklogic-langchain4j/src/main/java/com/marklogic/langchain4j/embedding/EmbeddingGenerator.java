/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.langchain4j.embedding;

import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.core.embedding.EmbeddingProducer;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class EmbeddingGenerator implements EmbeddingProducer {

    // We don't have any use for metadata, so just need a single instance for constructing text segments.
    private static final Metadata TEXT_SEGMENT_METADATA = new Metadata();

    private final EmbeddingModel embeddingModel;
    private final int batchSize;

    // Only used for debug logging.
    private static final AtomicLong tokenCount = new AtomicLong(0);
    private static final AtomicLong requestCount = new AtomicLong(0);

    private List<DocumentInputs> pendingInputs = new ArrayList<>();

    public EmbeddingGenerator(EmbeddingModel embeddingModel, int batchSize) {
        this.embeddingModel = embeddingModel;
        this.batchSize = batchSize;
    }

    @Override
    public List<DocumentInputs> produceEmbeddings(DocumentInputs newInputs) {
        final List<DocumentInputs> inputsToReturn = new ArrayList<>();
        final List<float[]> embeddingsForNewInputs = new ArrayList<>();

        final List<TextSegment> textSegments = collectPendingChunks();
        // Start adding chunks from the new inputs until we have enough to meet embedder batch size.
        boolean generatedEmbeddingsAtLeastOnce = false;
        for (String chunk : newInputs.getTextSegmentsToGenerateEmbeddings()) {
            textSegments.add(new TextSegment(chunk, TEXT_SEGMENT_METADATA));
            if (textSegments.size() >= batchSize) {
                List<Embedding> embeddings = generateEmbeddings(textSegments);
                assignEmbeddings(embeddings, embeddingsForNewInputs);
                textSegments.clear();

                if (!generatedEmbeddingsAtLeastOnce) {
                    generatedEmbeddingsAtLeastOnce = true;
                    // We know we're done with the pending inputs, so add them as inputs to return and clear out the list.
                    inputsToReturn.addAll(pendingInputs);
                    pendingInputs.clear();
                }
            }
        }

        // If we have at least one document to return, that means we sent at least 1 of the chunks in newInputs.
        // We don't want to leave newInputs in a partial state, so we flush the rest of the text segments.
        if (generatedEmbeddingsAtLeastOnce) {
            if (!textSegments.isEmpty()) {
                List<Embedding> embeddings = generateEmbeddings(textSegments);
                assignEmbeddings(embeddings, embeddingsForNewInputs);
            }
            newInputs.setGeneratedEmbeddings(embeddingsForNewInputs);
            inputsToReturn.add(newInputs);
        } else {
            // We didn't send anything to the embedding model, so add the new inputs to the list of pending inputs.
            pendingInputs.add(newInputs);
        }

        return inputsToReturn;
    }

    @Override
    public List<DocumentInputs> flush() {
        List<DocumentInputs> inputsToReturn = new ArrayList<>();
        if (pendingInputs.isEmpty()) {
            return inputsToReturn;
        }

        List<TextSegment> textSegments = collectPendingChunks();
        if (!textSegments.isEmpty()) {
            List<Embedding> embeddings = generateEmbeddings(textSegments);
            assignEmbeddings(embeddings, null);
        }

        inputsToReturn.addAll(pendingInputs);
        pendingInputs.clear();

        return inputsToReturn;
    }

    private List<Embedding> generateEmbeddings(List<TextSegment> textSegments) {
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Embedding segments, count: {}", textSegments.size());
        }
        Response<List<Embedding>> response = embeddingModel.embedAll(textSegments);
        logResponse(response, textSegments);
        return response.content();
    }

    private void assignEmbeddings(List<Embedding> embeddings, List<float[]> embeddingsForNewInputs) {
        int embeddingCounter = 0;
        // Assign embeddings to the pending inputs first.
        for (DocumentInputs pendingInput : pendingInputs) {
            int chunkCount = pendingInput.getTextSegmentsToGenerateEmbeddings().size();
            List<float[]> embeddingsForPendingInput = new ArrayList<>(chunkCount);
            for (; embeddingCounter < embeddings.size(); embeddingCounter++) {
                embeddingsForPendingInput.add(embeddings.get(embeddingCounter).vector());
            }
            pendingInput.setGeneratedEmbeddings(embeddingsForPendingInput);
        }

        // Any remaining embeddings should go into the ones for the newInputs.
        for (; embeddingCounter < embeddings.size(); embeddingCounter++) {
            embeddingsForNewInputs.add(embeddings.get(embeddingCounter).vector());
        }
    }

    // Probably want to cache this as well.
    private List<TextSegment> collectPendingChunks() {
        List<TextSegment> textSegments = new ArrayList<>();
        for (DocumentInputs pendingInput : pendingInputs) {
            for (String chunk : pendingInput.getTextSegmentsToGenerateEmbeddings()) {
                textSegments.add(new TextSegment(chunk, TEXT_SEGMENT_METADATA));
            }
        }
        return textSegments;
    }

    private void logResponse(Response<List<Embedding>> response, List<TextSegment> textSegments) {
        if (EmbeddingUtil.LANGCHAIN4J_LOGGER.isInfoEnabled()) {
            // Not every embedding model provides token usage.
            if (response.tokenUsage() != null) {
                EmbeddingUtil.LANGCHAIN4J_LOGGER.info("Sent {} chunks; token usage: {}", textSegments.size(), response.tokenUsage());
            } else {
                EmbeddingUtil.LANGCHAIN4J_LOGGER.info("Sent {} chunks", textSegments.size());
            }

            if (EmbeddingUtil.LANGCHAIN4J_LOGGER.isDebugEnabled()) {
                long totalRequests = requestCount.incrementAndGet();
                if (response.tokenUsage() != null) {
                    EmbeddingUtil.LANGCHAIN4J_LOGGER.debug("Requests: {}; tokens: {}", totalRequests,
                        tokenCount.addAndGet(response.tokenUsage().inputTokenCount())
                    );
                } else {
                    EmbeddingUtil.LANGCHAIN4J_LOGGER.debug("Requests: {}", totalRequests);
                }
            }
        }
    }
}
