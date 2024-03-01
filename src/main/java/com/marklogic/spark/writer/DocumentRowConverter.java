package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Knows how to build a document from a row corresponding to our {@code DocumentRowSchema}.
 */
class DocumentRowConverter implements RowConverter {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Optional<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        String uri = row.getString(0);
        BytesHandle content = new BytesHandle(row.getBinary(1));

        ObjectNode columnValues = objectMapper.createObjectNode();
        columnValues.put("URI", uri);
        columnValues.put("format", row.getString(2));

        DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
        return Optional.of(new DocBuilder.DocumentInputs(uri, content, columnValues, metadata));
    }

    @Override
    public List<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return new ArrayList<>();
    }
}
