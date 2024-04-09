package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    public Optional<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        final String path = row.getString(writeContext.getFileSchemaPathPosition());
        BytesHandle contentHandle = new BytesHandle(row.getBinary(writeContext.getFileSchemaContentPosition()));
        forceFormatIfNecessary(contentHandle);
        Optional<JsonNode> uriTemplateValues = deserializeContentToJson(path, contentHandle, row);
        return Optional.of(new DocBuilder.DocumentInputs(path, contentHandle, uriTemplateValues.orElse(null), null));
    }

    @Override
    public List<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return new ArrayList<>();
    }

    private void forceFormatIfNecessary(BytesHandle content) {
        if (writeContext.hasOption(Options.WRITE_FILE_ROWS_DOCUMENT_TYPE)) {
            String value = writeContext.getProperties().get(Options.WRITE_FILE_ROWS_DOCUMENT_TYPE);
            try {
                content.withFormat(Format.valueOf(value.toUpperCase()));
            } catch (IllegalArgumentException e) {
                String message = "Invalid value for %s: %s; must be one of 'JSON', 'XML', or 'TEXT'.";
                String optionAlias = writeContext.getOptionNameForMessage(Options.WRITE_FILE_ROWS_DOCUMENT_TYPE);
                throw new ConnectorException(String.format(message, optionAlias, value));
            }
        }
    }

    private Optional<JsonNode> deserializeContentToJson(String path, BytesHandle contentHandle, InternalRow row) {
        if (this.uriTemplate == null || this.uriTemplate.trim().length() == 0) {
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
