/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.document;

import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.validation.constraints.NotNull;

public abstract class DocumentRowSchema {

    public static final StructType SCHEMA = new StructType()
        .add("URI", DataTypes.StringType)
        .add("content", DataTypes.BinaryType)
        .add("format", DataTypes.StringType)
        .add("collections", DataTypes.createArrayType(DataTypes.StringType))
        .add("permissions", DataTypes.createMapType(
            DataTypes.StringType,
            DataTypes.createArrayType(DataTypes.StringType))
        )
        .add("quality", DataTypes.IntegerType)
        .add("properties", DataTypes.StringType)
        .add("metadataValues", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

    private DocumentRowSchema() {
    }

    /**
     * @param schema
     * @return true if the given schema has the same set of fields as this class's schema, while allowing for the
     * given schema to have additional fields as well, such as in the case of extracted text being added to the row.
     */
    public static boolean hasDocumentFields(StructType schema) {
        StructField[] otherFields = schema.fields();
        final int thisSchemaLength = SCHEMA.length();
        if (otherFields.length < thisSchemaLength) {
            return false;
        }
        StructField[] myFields = SCHEMA.fields();
        for (int i = 0; i < thisSchemaLength; i++) {
            if (!myFields[i].name().equals(otherFields[i].name())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given a row that conforms to this class's schema, return a {@code DocumentMetadataHandle} that contains the
     * metadata from the given row.
     *
     * @param row
     * @return
     */
    public static DocumentMetadataHandle makeDocumentMetadata(InternalRow row) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        addCollectionsToMetadata(row, metadata);
        addPermissionsToMetadata(row, metadata);
        if (!row.isNullAt(5)) {
            metadata.setQuality(row.getInt(5));
        }
        addPropertiesToMetadata(row, metadata);
        addMetadataValuesToMetadata(row, metadata);
        return metadata;
    }

    private static void addCollectionsToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
        if (!row.isNullAt(3)) {
            @NotNull ArrayData collections = row.getArray(3);
            for (int i = 0; i < collections.numElements(); i++) {
                @NotNull Object value = collections.get(i, DataTypes.StringType);
                metadata.getCollections().add(value.toString());
            }
        }
    }

    private static void addPermissionsToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
        if (!row.isNullAt(4)) {
            @NotNull MapData permissions = row.getMap(4);
            ArrayData roles = permissions.keyArray();
            ArrayData capabilities = permissions.valueArray();
            for (int i = 0; i < roles.numElements(); i++) {
                @NotNull Object role = roles.get(i, DataTypes.StringType);
                ArrayData caps = capabilities.getArray(i);
                DocumentMetadataHandle.Capability[] capArray = new DocumentMetadataHandle.Capability[caps.numElements()];
                for (int j = 0; j < caps.numElements(); j++) {
                    @NotNull Object value = caps.get(j, DataTypes.StringType);
                    capArray[j] = DocumentMetadataHandle.Capability.valueOf(value.toString().toUpperCase());
                }
                metadata.getPermissions().add(role.toString(), capArray);
            }
        }
    }

    private static void addPropertiesToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
        if (!row.isNullAt(6)) {
            String propertiesXml = row.getString(6);
            String metadataXml = String.format("<rapi:metadata xmlns:rapi='http://marklogic.com/rest-api'>%s</rapi:metadata>", propertiesXml);
            DocumentMetadataHandle tempMetadata = new DocumentMetadataHandle();
            tempMetadata.fromBuffer(metadataXml.getBytes());
            metadata.setProperties(tempMetadata.getProperties());
        }
    }

    private static void addMetadataValuesToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
        if (!row.isNullAt(7)) {
            @NotNull MapData properties = row.getMap(7);
            ArrayData keys = properties.keyArray();
            ArrayData values = properties.valueArray();
            for (int i = 0; i < keys.numElements(); i++) {
                Object key = keys.get(i, DataTypes.StringType);
                Object value = values.get(i, DataTypes.StringType);
                if (key != null && value != null) {
                    metadata.getMetadataValues().put(key.toString(), value.toString());
                }
            }
        }
    }
}
