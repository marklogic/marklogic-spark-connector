/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.JacksonHandle;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentSplitter;
import dev.langchain4j.data.segment.TextSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Let's assume for this that we make a sidecar - either one for each segment, or one
 * with all segments. Because we know we don't want to modify an unstructured text document.
 */
public class TextDocumentSplitter implements Function<List<DocumentWriteOperation>, List<DocumentWriteOperation>> {

    private final DocumentSplitter documentSplitter;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TextDocumentSplitter(DocumentSplitter documentSplitter) {
        this.documentSplitter = documentSplitter;
    }

    @Override
    public List<DocumentWriteOperation> apply(List<DocumentWriteOperation> documents) {
        List<DocumentWriteOperation> output = new ArrayList<>();
        documents.forEach(doc -> {
            String text = HandleAccessor.contentAsString(doc.getContent());
            List<TextSegment> segments = documentSplitter.split(new Document(text));
            for (int i = 0; i < segments.size(); i++) {
                ObjectNode node = objectMapper.createObjectNode();
                node.put("uri", doc.getUri());
                node.put("text", segments.get(i).text());
                final String uri = makeNewUri(doc.getUri(), i);
                output.add(new DocumentWriteOperationImpl(uri, doc.getMetadata(), new JacksonHandle(node)));
            }
        });
        return output;
    }

    private String makeNewUri(String uri, int index) {
        if (uri.endsWith(".json")) {
            return uri.substring(0, uri.lastIndexOf(".json")) + "-" + index + ".json";
        }
        return uri + "-" + index + ".json";
    }
}
