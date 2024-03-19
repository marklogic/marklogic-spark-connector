package com.marklogic.spark.reader.document;

import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import javax.xml.namespace.QName;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
        .add("properties", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
        .add("metadataValues", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

    private DocumentRowSchema() {
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

    public static void populateCollectionsColumn(Object[] row, DocumentMetadataHandle metadata) {
        UTF8String[] collections = new UTF8String[metadata.getCollections().size()];
        Iterator<String> iterator = metadata.getCollections().iterator();
        for (int i = 0; i < collections.length; i++) {
            collections[i] = UTF8String.fromString(iterator.next());
        }
        row[3] = ArrayData.toArrayData(collections);
    }

    public static void populatePermissionsColumn(Object[] row, DocumentMetadataHandle metadata) {
        DocumentMetadataHandle.DocumentPermissions perms = metadata.getPermissions();
        UTF8String[] roles = new UTF8String[perms.size()];
        Object[] capabilityArrays = new Object[perms.size()];
        int i = 0;
        for (Map.Entry<String, Set<DocumentMetadataHandle.Capability>> entry : perms.entrySet()) {
            roles[i] = UTF8String.fromString(entry.getKey());
            UTF8String[] capabilities = new UTF8String[entry.getValue().size()];
            int j = 0;
            Iterator<DocumentMetadataHandle.Capability> iterator = entry.getValue().iterator();
            while (iterator.hasNext()) {
                capabilities[j++] = UTF8String.fromString(iterator.next().name());
            }
            capabilityArrays[i++] = ArrayData.toArrayData(capabilities);
        }
        row[4] = ArrayBasedMapData.apply(roles, capabilityArrays);
    }

    public static void populateQualityColumn(Object[] row, DocumentMetadataHandle metadata) {
        row[5] = metadata.getQuality();
    }

    public static void populatePropertiesColumn(Object[] row, DocumentMetadataHandle metadata) {
        DocumentMetadataHandle.DocumentProperties props = metadata.getProperties();
        UTF8String[] keys = new UTF8String[props.size()];
        UTF8String[] values = new UTF8String[props.size()];
        int index = 0;
        for (QName key : props.keySet()) {
            keys[index] = UTF8String.fromString(key.toString());
            values[index++] = UTF8String.fromString(props.get(key, String.class));
        }
        row[6] = ArrayBasedMapData.apply(keys, values);
    }

    public static void populateMetadataValuesColumn(Object[] row, DocumentMetadataHandle metadata) {
        DocumentMetadataHandle.DocumentMetadataValues metadataValues = metadata.getMetadataValues();
        UTF8String[] keys = new UTF8String[metadataValues.size()];
        UTF8String[] values = new UTF8String[metadataValues.size()];
        int index = 0;
        for (Map.Entry<String, String> entry : metadataValues.entrySet()) {
            keys[index] = UTF8String.fromString(entry.getKey());
            values[index++] = UTF8String.fromString(entry.getValue());
        }
        row[7] = ArrayBasedMapData.apply(keys, values);
    }

    private static void addCollectionsToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
        if (!row.isNullAt(3)) {
            ArrayData collections = row.getArray(3);
            for (int i = 0; i < collections.numElements(); i++) {
                String value = collections.get(i, DataTypes.StringType).toString();
                metadata.getCollections().add(value);
            }
        }
    }

    private static void addPermissionsToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
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

    private static void addPropertiesToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
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

    private static void addMetadataValuesToMetadata(InternalRow row, DocumentMetadataHandle metadata) {
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
