/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.JsonRowSerializer;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Handles building a document from an "arbitrary" row - i.e. one with an unknown schema, where the row will be
 * serialized by Spark to a JSON object.
 */
class ArbitraryRowConverter implements RowConverter {

    private static final String MARKLOGIC_SPARK_FILE_PATH_COLUMN_NAME = "marklogic_spark_file_path";

    private final ObjectMapper objectMapper;
    private final XmlMapper xmlMapper;
    private final JsonRowSerializer jsonRowSerializer;
    private final String uriTemplate;
    private final String jsonRootName;
    private final String xmlRootName;
    private final String xmlNamespace;
    private final int filePathIndex;

    ArbitraryRowConverter(WriteContext writeContext) {
        this.filePathIndex = determineFilePathIndex(writeContext.getSchema());
        this.uriTemplate = writeContext.getStringOption(Options.WRITE_URI_TEMPLATE);
        this.jsonRootName = writeContext.getStringOption(Options.WRITE_JSON_ROOT_NAME);
        this.xmlRootName = writeContext.getStringOption(Options.WRITE_XML_ROOT_NAME);
        this.xmlNamespace = writeContext.getStringOption(Options.WRITE_XML_NAMESPACE);
        this.objectMapper = new ObjectMapper();
        this.xmlMapper = this.xmlRootName != null ? new XmlMapper() : null;
        this.jsonRowSerializer = new JsonRowSerializer(writeContext.getSchema(), writeContext.getProperties());
    }

    @Override
    public Iterator<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        String initialUri = null;
        if (this.filePathIndex > -1) {
            initialUri = row.getString(this.filePathIndex) + "/" + UUID.randomUUID();
            row.setNullAt(this.filePathIndex);
        }

        final String json = this.jsonRowSerializer.serializeRowToJson(row);

        AbstractWriteHandle contentHandle = null;
        ObjectNode deserializedJson = null;
        ObjectNode uriTemplateValues = null;
        final boolean mustRemoveFilePathField = this.filePathIndex > 1 && jsonRowSerializer.isIncludeNullFields();

        if (this.jsonRootName != null || this.xmlRootName != null || this.uriTemplate != null || mustRemoveFilePathField) {
            deserializedJson = readTree(json);
            if (mustRemoveFilePathField) {
                deserializedJson.remove(MARKLOGIC_SPARK_FILE_PATH_COLUMN_NAME);
            }
        }

        if (this.uriTemplate != null) {
            uriTemplateValues = deserializedJson;
        }

        if (this.jsonRootName != null) {
            ObjectNode jsonObjectWithRootName = objectMapper.createObjectNode();
            jsonObjectWithRootName.set(jsonRootName, deserializedJson);
            contentHandle = new JacksonHandle(jsonObjectWithRootName);
            if (this.uriTemplate != null) {
                uriTemplateValues = jsonObjectWithRootName;
            }
        }

        if (contentHandle == null) {
            // If the user wants XML, then we've definitely deserialized the JSON and removed the file path if
            // needed. So use that JsonNode to produce an XML string.
            if (xmlRootName != null) {
                contentHandle = new StringHandle(convertJsonToXml(deserializedJson)).withFormat(Format.XML);
            }
            // If we've already gone to the effort of creating deserializedJson, use it for the content.
            else if (deserializedJson != null) {
                contentHandle = new JacksonHandle(deserializedJson);
            } else {
                // Simplest scenario where we never have a reason to incur the expense of deserializing the JSON string,
                // so we can just use StringHandle.
                contentHandle = new StringHandle(json).withFormat(Format.JSON);
            }
        }

        return Stream.of(new DocBuilder.DocumentInputs(initialUri, contentHandle, uriTemplateValues, null)).iterator();
    }

    @Override
    public Iterator<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return Stream.<DocBuilder.DocumentInputs>empty().iterator();
    }

    /**
     * A Spark user can add a column via:
     * withColumn("marklogic_spark_file_path", new Column("_metadata.file_path"))
     * <p>
     * This allows access to the file path when using a Spark data source - e.g. CSV, Parquet - to read a file.
     * The column will be used to generate an initial URI for the corresponding document, and the column will then
     * be removed after that so that it's not included in the document.
     *
     * @return
     */
    private int determineFilePathIndex(StructType schema) {
        StructField[] fields = schema.fields();
        for (int i = 0; i < fields.length; i++) {
            if (MARKLOGIC_SPARK_FILE_PATH_COLUMN_NAME.equals(fields[i].name())) {
                return i;
            }
        }
        return -1;
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

    /**
     * jackson-xml-mapper unfortunately does not yet support a root namespace. Nor does it allow for the root element
     * to be omitted. So we always end up with "ObjectNode" as a root element. See
     * https://github.com/FasterXML/jackson-dataformat-xml/issues/541 for more information. So this method does some
     * work to replace that root element with one based on user inputs.
     *
     * @param doc
     * @return
     */
    private String convertJsonToXml(JsonNode doc) {
        try {
            String xml = xmlMapper.writer().writeValueAsString(doc);
            String startTag = this.xmlNamespace != null ?
                String.format("<%s xmlns='%s'>", this.xmlRootName, this.xmlNamespace) :
                String.format("<%s>", this.xmlRootName);
            return new StringBuilder(startTag)
                .append(xml.substring("<ObjectNode>".length(), xml.length() - "</ObjectNode>".length()))
                .append(String.format("</%s>", this.xmlRootName))
                .toString();
        } catch (JsonProcessingException e) {
            // We don't expect this occur; Jackson should be able to convert any JSON object that it created into
            // a valid XML document.
            throw new ConnectorException(String.format("Unable to convert JSON to XML for doc: %s", doc), e);
        }
    }
}
