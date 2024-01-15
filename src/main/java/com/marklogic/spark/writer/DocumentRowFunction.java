package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataTypes;

import javax.xml.namespace.QName;
import java.util.function.Function;

/**
 * Knows how to build a document from a row corresponding to our {@code DocumentRowSchema}.
 */
class DocumentRowFunction implements Function<InternalRow, DocBuilder.DocumentInputs> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public DocBuilder.DocumentInputs apply(InternalRow row) {
        String uri = row.getString(0);
        BytesHandle content = new BytesHandle(row.getBinary(1));

        ObjectNode columnValues = objectMapper.createObjectNode();
        columnValues.put("URI", uri);
        columnValues.put("format", row.getString(2));

        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        addCollectionsToMetadata(row, metadata);
        addPermissionsToMetadata(row, metadata);
        if (!row.isNullAt(5)) {
            metadata.setQuality(row.getInt(5));
        }
        addPropertiesToMetadata(row, metadata);
        addMetadataValuesToMetadata(row, metadata);
        return new DocBuilder.DocumentInputs(uri, content, columnValues, metadata);
    }

    private void addCollectionsToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
        if (!row.isNullAt(3)) {
            ArrayData collections = row.getArray(3);
            for (int i = 0; i < collections.numElements(); i++) {
                String value = collections.get(i, DataTypes.StringType).toString();
                metadata.getCollections().add(value);
            }
        }
    }

    private void addPermissionsToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
        if (!row.isNullAt(4)) {
            MapData permissions = row.getMap(4);
            ArrayData roles = permissions.keyArray();
            ArrayData capabilities = permissions.valueArray();
            for (int i = 0; i < roles.numElements(); i++) {
                String role = roles.get(i, DataTypes.StringType).toString();
                ArrayData caps = capabilities.getArray(i);
                DocumentMetadataHandle.Capability[] capArray = new DocumentMetadataHandle.Capability[caps.numElements()];
                for (int j = 0; j < caps.numElements(); j++) {
                    String value = caps.get(j, DataTypes.StringType).toString();
                    capArray[j] = DocumentMetadataHandle.Capability.valueOf(value.toUpperCase());
                }
                metadata.getPermissions().add(role, capArray);
            }
        }
    }

    private void addPropertiesToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
        if (!row.isNullAt(6)) {
            MapData properties = row.getMap(6);
            ArrayData qnames = properties.keyArray();
            ArrayData values = properties.valueArray();
            for (int i = 0; i < qnames.numElements(); i++) {
                String qname = qnames.get(i, DataTypes.StringType).toString();
                String value = values.get(i, DataTypes.StringType).toString();
                metadata.getProperties().put(QName.valueOf(qname), value);
            }
        }
    }

    private void addMetadataValuesToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
        if (!row.isNullAt(7)) {
            MapData properties = row.getMap(7);
            ArrayData keys = properties.keyArray();
            ArrayData values = properties.valueArray();
            for (int i = 0; i < keys.numElements(); i++) {
                String key = keys.get(i, DataTypes.StringType).toString();
                String value = values.get(i, DataTypes.StringType).toString();
                metadata.getMetadataValues().put(key, value);
            }
        }
    }
}
