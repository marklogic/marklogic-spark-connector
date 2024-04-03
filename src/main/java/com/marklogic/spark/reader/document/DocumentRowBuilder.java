package com.marklogic.spark.reader.document;

import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

import javax.xml.namespace.QName;
import java.util.*;

/**
 * Knows how to build a Spark row conforming to our {@code DocumentRowSchema}.
 * <p>
 * This has to support two different ways of specifying which metadata to include. {@code ForestReader} needs to
 * capture the requested metadata in one way, while other approaches can just capture the metadata categories as a
 * simple list of strings.
 */
public class DocumentRowBuilder {

    private final List<String> metadataCategories;
    private final Set<DocumentManager.Metadata> requestedMetadata;

    private String uri;
    private byte[] content;
    private String format;
    private DocumentMetadataHandle metadata;

    public DocumentRowBuilder(List<String> metadataCategories) {
        this.metadataCategories = metadataCategories != null ? metadataCategories : new ArrayList<>();
        this.requestedMetadata = null;
    }

    public DocumentRowBuilder(Set<DocumentManager.Metadata> requestedMetadata) {
        this.requestedMetadata = requestedMetadata;
        this.metadataCategories = null;
    }

    public DocumentRowBuilder withUri(String uri) {
        this.uri = uri;
        return this;
    }

    public DocumentRowBuilder withContent(byte[] content) {
        this.content = content;
        return this;
    }

    public DocumentRowBuilder withFormat(String format) {
        this.format = format;
        return this;
    }

    public DocumentRowBuilder withMetadata(DocumentMetadataHandle metadata) {
        this.metadata = metadata;
        return this;
    }

    public GenericInternalRow buildRow() {
        Object[] row = new Object[8];
        row[0] = UTF8String.fromString(uri);
        row[1] = ByteArray.concat(content);
        if (format != null) {
            row[2] = UTF8String.fromString(format);
        }
        if (metadata != null) {
            if (includeCollections()) {
                populateCollectionsColumn(row, metadata);
            }
            if (includePermissions()) {
                populatePermissionsColumn(row, metadata);
            }
            if (includeQuality()) {
                populateQualityColumn(row, metadata);
            }
            if (includeProperties()) {
                populatePropertiesColumn(row, metadata);
            }
            if (includeMetadataValues()) {
                populateMetadataValuesColumn(row, metadata);
            }
        }
        return new GenericInternalRow(row);
    }
    
    private boolean includeCollections() {
        return includeMetadata("collections", DocumentManager.Metadata.COLLECTIONS);
    }

    private boolean includePermissions() {
        return includeMetadata("permissions", DocumentManager.Metadata.PERMISSIONS);
    }

    private boolean includeQuality() {
        return includeMetadata("quality", DocumentManager.Metadata.QUALITY);
    }

    private boolean includeProperties() {
        return includeMetadata("properties", DocumentManager.Metadata.PROPERTIES);
    }

    private boolean includeMetadataValues() {
        return includeMetadata("metadatavalues", DocumentManager.Metadata.METADATAVALUES);
    }

    private boolean includeMetadata(String categoryName, DocumentManager.Metadata metadataType) {
        return metadataCategories != null ?
            metadataCategories.contains(categoryName) || metadataCategories.isEmpty() :
            requestedMetadata.contains(metadataType) || requestedMetadata.contains(DocumentManager.Metadata.ALL);
    }

    private void populateCollectionsColumn(Object[] row, DocumentMetadataHandle metadata) {
        UTF8String[] collections = new UTF8String[metadata.getCollections().size()];
        Iterator<String> iterator = metadata.getCollections().iterator();
        for (int i = 0; i < collections.length; i++) {
            collections[i] = UTF8String.fromString(iterator.next());
        }
        row[3] = ArrayData.toArrayData(collections);
    }

    private void populatePermissionsColumn(Object[] row, DocumentMetadataHandle metadata) {
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

    private void populateQualityColumn(Object[] row, DocumentMetadataHandle metadata) {
        row[5] = metadata.getQuality();
    }

    private void populatePropertiesColumn(Object[] row, DocumentMetadataHandle metadata) {
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

    private void populateMetadataValuesColumn(Object[] row, DocumentMetadataHandle metadata) {
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
}
