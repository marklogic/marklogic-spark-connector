package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Knows how to build a document from a row corresponding to our {@code FileRowSchema}.
 */
class FileRowConverter implements RowConverter {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private WriteContext writeContext;

    FileRowConverter(WriteContext writeContext) {
        this.writeContext = writeContext;
    }

    @Override
    public Optional<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        String initialUri = row.getString(writeContext.getFileSchemaPathPosition());
        BytesHandle content = new BytesHandle(row.getBinary(writeContext.getFileSchemaContentPosition()));
        forceFormatIfNecessary(content);
        JsonNode uriTemplateValues = null;
        if (writeContext.hasOption(Options.WRITE_URI_TEMPLATE)) {
            try {
                uriTemplateValues = objectMapper.readTree(row.getBinary(3));
            } catch (IOException e) {
                throw new ConnectorException("Can't read your JSON! " + e.getMessage(), e);
            }
        }
        return Optional.of(new DocBuilder.DocumentInputs(initialUri, content, uriTemplateValues, null));
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
                String message = "Invalid value for option %s: %s; must be one of 'JSON', 'XML', or 'TEXT'.";
                throw new ConnectorException(String.format(message, Options.WRITE_FILE_ROWS_DOCUMENT_TYPE, value));
            }
        }
    }
}
