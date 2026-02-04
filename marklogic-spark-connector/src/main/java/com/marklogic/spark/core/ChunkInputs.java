/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Encapsulates the data associated with a chunk of text, including its embedding and classification. Note there's
 * some naming issues to work out with this class and the Chunk interface.
 */
public class ChunkInputs {

    private final String text;
    private float[] embedding;
    private byte[] classification;
    private String modelName;
    private JsonNode metadata;

    public ChunkInputs(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public float[] getEmbedding() {
        return embedding;
    }

    public void setEmbedding(float[] embedding) {
        this.embedding = embedding;
    }

    public byte[] getClassification() {
        return classification;
    }

    public void setClassification(byte[] classification) {
        this.classification = classification;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public JsonNode getMetadata() {
        return metadata;
    }

    public void setMetadata(JsonNode metadata) {
        this.metadata = metadata;
    }
}
