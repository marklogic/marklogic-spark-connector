package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;

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
        ObjectNode columnValues = null;
        if (writeContext.hasOption(Options.WRITE_URI_TEMPLATE)) {
            columnValues = objectMapper.createObjectNode();
            columnValues.put("path", initialUri);
            Object modificationTime = row.get(1, DataTypes.LongType);
            if (modificationTime != null) {
                columnValues.put("modificationTime", modificationTime.toString());
            }
            columnValues.put("length", row.getLong(2));
            // Not including content as it's a byte array that is not expected to be helpful for making a URI.
        }
        return Optional.of(new DocBuilder.DocumentInputs(initialUri, content, columnValues, null));
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
