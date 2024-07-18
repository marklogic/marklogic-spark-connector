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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;
import org.json.XML;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Handles building a document from an "arbitrary" row - i.e. one with an unknown schema, where the row will be
 * serialized by Spark to a JSON object.
 */
class ArbitraryRowConverter implements RowConverter {

    private static final String MARKLOGIC_SPARK_FILE_PATH_COLUMN_NAME = "marklogic_spark_file_path";

    private final ObjectMapper objectMapper;

    private final StructType schema;
    private final String uriTemplate;
    private final String jsonRootName;
    private final String xmlRootName;
    private final String xmlNamespace;

    private final int filePathIndex;

    ArbitraryRowConverter(WriteContext writeContext) {
        this.schema = writeContext.getSchema();
        this.filePathIndex = determineFilePathIndex();

        this.uriTemplate = writeContext.getStringOption(Options.WRITE_URI_TEMPLATE);
        this.jsonRootName = writeContext.getStringOption(Options.WRITE_JSON_ROOT_NAME);
        this.xmlRootName = writeContext.getStringOption(Options.WRITE_XML_ROOT_NAME);
        this.xmlNamespace = writeContext.getStringOption(Options.WRITE_XML_NAMESPACE);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Optional<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        String initialUri = null;
        if (this.filePathIndex > -1) {
            initialUri = row.getString(this.filePathIndex) + "/" + UUID.randomUUID();
            row.setNullAt(this.filePathIndex);
        }

        final String json = convertRowToJSONString(row);
        AbstractWriteHandle contentHandle = this.xmlRootName != null ?
            new StringHandle(convertJsonToXml(json)).withFormat(Format.XML) :
            new StringHandle(json).withFormat(Format.JSON);

        ObjectNode uriTemplateValues = null;
        if (this.uriTemplate != null || this.jsonRootName != null) {
            ObjectNode jsonObject = readTree(json);
            if (this.uriTemplate != null) {
                uriTemplateValues = jsonObject;
            }
            if (this.jsonRootName != null) {
                ObjectNode root = objectMapper.createObjectNode();
                root.set(jsonRootName, jsonObject);
                contentHandle = new JacksonHandle(root);
                if (this.uriTemplate != null) {
                    uriTemplateValues = root;
                }
            }
        }
        return Optional.of(new DocBuilder.DocumentInputs(initialUri, contentHandle, uriTemplateValues, null));
    }

    @Override
    public List<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return new ArrayList<>();
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
    private int determineFilePathIndex() {
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

    private String convertRowToJSONString(InternalRow row) {
        StringWriter writer = new StringWriter();
        JacksonGenerator jacksonGenerator = new JacksonGenerator(this.schema, writer, Util.DEFAULT_JSON_OPTIONS);
        jacksonGenerator.write(row);
        jacksonGenerator.flush();
        return writer.toString();
    }

    /**
     * jackson-xml-mapper unfortunately does not yet support a root namespace. Nor does it allow for the root element
     * to be omitted. So we always end up with "ObjectNode" as a root element. See
     * https://github.com/FasterXML/jackson-dataformat-xml/issues/541 for more information.
     * <p>
     * While JSON-Java does not support a root namespace, it does allow for the root element to be omitted. That is
     * sufficient for us, as we can then generate our own root element - albeit via string concatentation - that
     * includes a user-defined namespace.
     *
     * @param json
     * @return
     */
    private String convertJsonToXml(String json) {
        JSONObject jsonObject = new JSONObject(json);
        if (this.xmlNamespace != null) {
            StringBuilder xml = new StringBuilder(String.format("<%s xmlns='%s'>", this.xmlRootName, this.xmlNamespace));
            xml.append(XML.toString(jsonObject, null));
            return xml.append(String.format("</%s>", this.xmlRootName)).toString();
        }
        return XML.toString(jsonObject, this.xmlRootName);
    }
}
