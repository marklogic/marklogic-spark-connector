/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.FileContext;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Knows how to build a document from a row corresponding to our {@code DocumentRowSchema}.
 */
class DocumentRowConverter implements RowConverter {

    private final ObjectMapper objectMapper;
    private final String uriTemplate;
    private final Format documentFormat;
    private final boolean isStreamingFromFiles;

    DocumentRowConverter(WriteContext writeContext) {
        this.uriTemplate = writeContext.getStringOption(Options.WRITE_URI_TEMPLATE);
        this.documentFormat = writeContext.getDocumentFormat();
        this.objectMapper = new ObjectMapper();
        this.isStreamingFromFiles = writeContext.hasOption(Options.STREAM_FILES);
    }

    @Override
    public Optional<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        final String uri = row.getString(0);

        final boolean isNakedProperties = row.isNullAt(1);
        if (isNakedProperties) {
            DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
            return Optional.of(new DocBuilder.DocumentInputs(uri, null, null, metadata));
        }

        Content content = this.isStreamingFromFiles ?
            readContentFromFile(uri, row) :
            readContentFromRow(uri, row);

        DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
        return Optional.of(new DocBuilder.DocumentInputs(
            uri, content.contentHandle, content.uriTemplateValues, metadata)
        );
    }

    @Override
    public List<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return new ArrayList<>();
    }

    private Content readContentFromRow(String uri, InternalRow row) {
        BytesHandle bytesHandle = new BytesHandle(row.getBinary(1));
        if (this.documentFormat != null) {
            bytesHandle.withFormat(this.documentFormat);
        }
        JsonNode uriTemplateValues = null;
        if (this.uriTemplate != null && this.uriTemplate.trim().length() > 0) {
            String format = row.isNullAt(2) ? null : row.getString(2);
            uriTemplateValues = deserializeContentToJson(uri, bytesHandle, format);
        }
        return new Content(bytesHandle, uriTemplateValues);
    }

    private JsonNode deserializeContentToJson(String initialUri, BytesHandle contentHandle, String format) {
        try {
            return objectMapper.readTree(contentHandle.get());
        } catch (IOException e) {
            // Preserves the initial support in the 2.2.0 release.
            ObjectNode values = objectMapper.createObjectNode();
            values.put("URI", initialUri);
            if (format != null) {
                values.put("format", format);
            }
            return values;
        }
    }

    /**
     * In a scenario where the user wants to stream a file into MarkLogic, the content column will contain a serialized
     * instance of {@code FileContext}, which is used to stream the file into a {@code InputStreamHandle}.
     */
    private Content readContentFromFile(String filePath, InternalRow row) {
        byte[] bytes = row.getBinary(1);
        String filePathInErrorMessage = filePath;
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            FileContext fileContext = (FileContext) ois.readObject();
            final String decodedPath = fileContext.decodeFilePath(filePath);
            filePathInErrorMessage = decodedPath;
            InputStreamHandle streamHandle = new InputStreamHandle(fileContext.openFile(decodedPath));
            if (this.documentFormat != null) {
                streamHandle.withFormat(this.documentFormat);
            }
            return new Content(streamHandle, null);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read from file %s; cause: %s", filePathInErrorMessage, e.getMessage()));
        }
    }

    private static class Content {
        private final AbstractWriteHandle contentHandle;
        private final JsonNode uriTemplateValues;

        public Content(AbstractWriteHandle contentHandle, JsonNode uriTemplateValues) {
            this.contentHandle = contentHandle;
            this.uriTemplateValues = uriTemplateValues;
        }

        AbstractWriteHandle getContentHandle() {
            return contentHandle;
        }

        JsonNode getUriTemplateValues() {
            return uriTemplateValues;
        }
    }
}
