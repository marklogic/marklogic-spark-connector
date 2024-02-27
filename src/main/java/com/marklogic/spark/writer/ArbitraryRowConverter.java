package com.marklogic.spark.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.json.JacksonGenerator;

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

    private WriteContext writeContext;

    ArbitraryRowConverter(WriteContext writeContext) {
        this.writeContext = writeContext;
    }

    @Override
    public Optional<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        String json = convertRowToJSONString(row);
        StringHandle content = new StringHandle(json).withFormat(Format.JSON);
        ObjectNode columnValues = null;
        if (writeContext.hasOption(Options.WRITE_URI_TEMPLATE)) {
            try {
                columnValues = (ObjectNode) objectMapper.readTree(json);
            } catch (JsonProcessingException e) {
                throw new ConnectorException(String.format("Unable to read JSON row: %s", e.getMessage()), e);
            }
        }
        return Optional.of(new DocBuilder.DocumentInputs(null, content, columnValues, null));
    }

    @Override
    public List<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return new ArrayList<>();
    }

    private String convertRowToJSONString(InternalRow row) {
        StringWriter jsonObjectWriter = new StringWriter();
        JacksonGenerator jacksonGenerator = new JacksonGenerator(
            this.writeContext.getSchema(),
            jsonObjectWriter,
            Util.DEFAULT_JSON_OPTIONS
        );
        jacksonGenerator.write(row);
        jacksonGenerator.flush();
        return jsonObjectWriter.toString();
    }
}
