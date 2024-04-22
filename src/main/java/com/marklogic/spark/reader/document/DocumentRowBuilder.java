package com.marklogic.spark.reader.document;

import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.spark.ConnectorException;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.XMLOutputter;

import java.io.ByteArrayInputStream;
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

    // For handling XML document properties
    private final SAXBuilder saxBuilder;
    private final XMLOutputter xmlOutputter;
    private static final Namespace PROPERTIES_NAMESPACE = Namespace.getNamespace("prop", "http://marklogic.com/xdmp/property");

    private String uri;
    private byte[] content;
    private String format;
    private DocumentMetadataHandle metadata;

    public DocumentRowBuilder(List<String> metadataCategories) {
        this.saxBuilder = new SAXBuilder();
        this.xmlOutputter = new XMLOutputter();
        this.metadataCategories = metadataCategories != null ? metadataCategories : new ArrayList<>();
        this.requestedMetadata = null;
    }

    public DocumentRowBuilder(Set<DocumentManager.Metadata> requestedMetadata) {
        this.saxBuilder = new SAXBuilder();
        this.xmlOutputter = new XMLOutputter();
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

    /**
     * The properties fragment can be a complex XML structure with mixed content and attributes and thus cannot be
     * defined as a map of particular types. Instead, as of the 2.3.0 release of the connector, the properties column
     * is of type String and is expected to contain a serialized string of XML representing the contents of the
     * properties fragment. To obtain that, this method serializes the metadata object into its REST API XML
     * serialization and then extracts the portion containing the document properties.
     *
     * @param row
     * @param metadata
     */
    private void populatePropertiesColumn(Object[] row, DocumentMetadataHandle metadata) {
        if (metadata.getProperties() == null || metadata.getProperties().size() == 0) {
            return;
        }
        try {
            Document doc = this.saxBuilder.build(new ByteArrayInputStream(metadata.toBuffer()));
            Element properties = doc.getRootElement().getChild("properties", PROPERTIES_NAMESPACE);
            if (properties != null) {
                row[6] = UTF8String.fromString(this.xmlOutputter.outputString(properties));
            }
        } catch (Exception e) {
            throw new ConnectorException(String.format(
                "Unable to process XML document properties for row with URI %s; cause: %s", row[0], e.getMessage()), e);
        }
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
