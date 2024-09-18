/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.segment.TextSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class JsonDocumentSplitter implements Function<List<DocumentWriteOperation>, List<DocumentWriteOperation>> {

    private final DocumentSplitter documentSplitter;
    private final String textFieldName;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonDocumentSplitter(DocumentSplitter documentSplitter, String textFieldName) {
        this.documentSplitter = documentSplitter;
        this.textFieldName = textFieldName;
    }

    @Override
    public List<DocumentWriteOperation> apply(List<DocumentWriteOperation> documents) {
        List<DocumentWriteOperation> output = new ArrayList<>();
        documents.forEach(doc -> {
            ObjectNode node = extractJson(doc);
            String text = node.get(textFieldName).asText();
            List<TextSegment> segments = documentSplitter.split(new Document(text));
            for (int i = 0; i < segments.size(); i++) {
                ObjectNode copy = node.deepCopy();
                copy.put(textFieldName, segments.get(i).text());
                final String uri = makeNewUri(doc.getUri(), i);
                output.add(new DocumentWriteOperationImpl(uri, doc.getMetadata(), new JacksonHandle(copy)));
            }
        });
        return output;
    }

    private ObjectNode extractJson(DocumentWriteOperation doc) {
        AbstractWriteHandle writeHandle = doc.getContent();
        if (writeHandle instanceof JacksonHandle) {
            return (ObjectNode) ((JacksonHandle) doc.getContent()).get();
        }
        String json = HandleAccessor.contentAsString(writeHandle);
        try {
            return (ObjectNode) objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String makeNewUri(String uri, int index) {
        if (uri.endsWith(".json")) {
            return uri.substring(0, uri.lastIndexOf(".json")) + "-" + index + ".json";
        }
        return uri + "-" + index + ".json";
    }
}
