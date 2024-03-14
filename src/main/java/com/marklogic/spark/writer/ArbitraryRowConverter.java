package com.marklogic.spark.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JacksonGenerator;
import org.apache.spark.sql.types.StructType;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Handles building a document from an "arbitrary" row - i.e. one with an unknown schema, where the row will be
 * serialized by Spark to a JSON object.
 */
class ArbitraryRowConverter implements RowConverter {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private final StructType schema;
    private final String uriTemplate;
    private final String jsonRootName;

    ArbitraryRowConverter(WriteContext writeContext) {
        this.schema = writeContext.getSchema();
        this.uriTemplate = writeContext.getStringOption(Options.WRITE_URI_TEMPLATE);
        this.jsonRootName = writeContext.getStringOption(Options.WRITE_JSON_ROOT_NAME);
    }

    @Override
    public Optional<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        final String json = convertRowToJSONString(row);
        AbstractWriteHandle contentHandle = new StringHandle(json).withFormat(Format.JSON);
        ObjectNode columnValues = null;
        if (this.uriTemplate != null || this.jsonRootName != null) {
            columnValues = readTree(json);
            if (this.jsonRootName != null) {
                ObjectNode root = objectMapper.createObjectNode();
                root.set(jsonRootName, columnValues);
                contentHandle = new JacksonHandle(root);
                columnValues = root;
            }
        }
        return Optional.of(new DocBuilder.DocumentInputs(null, contentHandle, columnValues, null));
    }

    @Override
    public List<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return new ArrayList<>();
    }

    private ObjectNode readTree(String json) {
        // We don't ever expect this to fail, as the JSON is produced by Spark's JacksonGenerator and should always
        // be valid JSON. But Jackson throws a checked exception, so gotta handle it.
        try {
            return (ObjectNode) objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new ConnectorException(String.format("Unable to read JSON row: %s", e.getMessage()), e);
        }
    }

    private String convertRowToJSONString(InternalRow row) {
        StringWriter writer = new StringWriter();
        JacksonGenerator jacksonGenerator = new JacksonGenerator(this.schema, writer, Util.DEFAULT_JSON_OPTIONS);
        jacksonGenerator.write(row);
        jacksonGenerator.flush();
        return writer.toString();
    }
}
