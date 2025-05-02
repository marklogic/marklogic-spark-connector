/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.Options;
import com.marklogic.spark.core.DocumentInputs;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Knows how to build a document from a row corresponding to our {@code FileRowSchema}.
 */
class FileRowConverter implements RowConverter {

    private final WriteContext writeContext;
    private final ObjectMapper objectMapper;
    private final String uriTemplate;

    FileRowConverter(WriteContext writeContext) {
        this.writeContext = writeContext;
        this.uriTemplate = writeContext.getStringOption(Options.WRITE_URI_TEMPLATE);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Iterator<DocumentInputs> convertRow(InternalRow row) {
        final String path = row.getString(writeContext.getFileSchemaPathPosition());
        BytesHandle contentHandle = new BytesHandle(row.getBinary(writeContext.getFileSchemaContentPosition()));
        forceFormatIfNecessary(contentHandle);
        Optional<JsonNode> uriTemplateValues = deserializeContentToJson(path, contentHandle, row);
        return Stream.of(new DocumentInputs(path, contentHandle, uriTemplateValues.orElse(null), null)).iterator();
    }

    @Override
    public Iterator<DocumentInputs> getRemainingDocumentInputs() {
        return Stream.<DocumentInputs>empty().iterator();
    }

    @SuppressWarnings({"deprecation", "removal"})
    private void forceFormatIfNecessary(BytesHandle content) {
        Format format = writeContext.getDocumentFormat();
        if (format != null) {
            content.withFormat(format);
        } else {
            format = writeContext.getDeprecatedFileRowsDocumentFormat();
            if (format != null) {
                content.withFormat(format);
            }
        }
    }

    private Optional<JsonNode> deserializeContentToJson(String path, BytesHandle contentHandle, InternalRow row) {
        if (this.uriTemplate == null || this.uriTemplate.trim().isEmpty()) {
            return Optional.empty();
        }
        try {
            JsonNode json = objectMapper.readTree(contentHandle.get());
            return Optional.of(json);
        } catch (IOException e) {
            // Preserves the initial support in the 2.2.0 release.
            ObjectNode values = objectMapper.createObjectNode();
            values.put("path", path);
            if (!row.isNullAt(1)) {
                values.put("modificationTime", row.get(1, DataTypes.LongType).toString());
            }
            if (!row.isNullAt(2)) {
                values.put("length", row.getLong(2));
            }
            return Optional.of(values);
        }
    }
}
