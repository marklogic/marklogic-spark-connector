package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.spark.sql.catalyst.InternalRow;

import javax.xml.XMLConstants;
import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Knows how to write the value in the "content" column of a row conforming to our {@code DocumentRowSchema}. Supports
 * pretty-printing as well. This keeps an instance of a JAXP Transformer, which is safe for one thread to use
 * multiple times.
 */
class ContentWriter {

    private final Transformer transformer;
    private final ObjectMapper objectMapper;
    private final boolean prettyPrint;
    private final Charset encoding;

    ContentWriter(Map<String, String> properties) {
        this.encoding = determineEncoding(properties);
        this.prettyPrint = "true".equalsIgnoreCase(properties.get(Options.WRITE_FILES_PRETTY_PRINT));
        if (this.prettyPrint) {
            this.objectMapper = new ObjectMapper();
            this.transformer = newTransformer();
        } else {
            this.transformer = null;
            this.objectMapper = null;
        }
    }

    void writeContent(InternalRow row, OutputStream outputStream) throws IOException {
        if (this.prettyPrint) {
            prettyPrintContent(row, outputStream);
        } else {
            byte[] bytes = row.getBinary(1);
            if (this.encoding != null) {
                // We know the string from MarkLogic is UTF-8, so we use getBytes to convert it to the user's
                // specified encoding (as opposed to new String(bytes, encoding)).
                outputStream.write(new String(bytes).getBytes(this.encoding));
            } else {
                outputStream.write(row.getBinary(1));
            }
        }
    }

    void writeMetadata(InternalRow row, OutputStream outputStream) throws IOException {
        String metadataXml = DocumentRowSchema.makeDocumentMetadata(row).toString();
        // Must honor the encoding here as well, as a user could easily have values that require encoding in metadata
        // values or in a properties fragment.
        if (this.encoding != null) {
            outputStream.write(metadataXml.getBytes(this.encoding));
        } else {
            outputStream.write(metadataXml.getBytes());
        }
    }

    private Charset determineEncoding(Map<String, String> properties) {
        String encodingValue = properties.get(Options.WRITE_FILES_ENCODING);
        if (encodingValue != null && encodingValue.trim().length() > 0) {
            try {
                return Charset.forName(encodingValue);
            } catch (Exception ex) {
                throw new ConnectorException(String.format("Unsupported encoding value: %s", encodingValue), ex);
            }
        }
        return null;
    }

    private Transformer newTransformer() {
        try {
            TransformerFactory factory = TransformerFactory.newInstance();
            // Disables certain features as recommended by Sonar to prevent security vulnerabilities.
            // Also see https://stackoverflow.com/questions/32178558/how-to-prevent-xml-external-entity-injection-on-transformerfactory .
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
            final Transformer t = factory.newTransformer();
            if (this.encoding != null) {
                t.setOutputProperty(OutputKeys.ENCODING, this.encoding.name());
            } else {
                t.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            }
            t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            t.setOutputProperty(OutputKeys.INDENT, "yes");
            return t;
        } catch (TransformerConfigurationException e) {
            throw new ConnectorException(
                String.format("Unable to instantiate transformer for pretty-printing XML; cause: %s", e.getMessage()), e
            );
        }
    }

    private void prettyPrintContent(InternalRow row, OutputStream outputStream) throws IOException {
        final byte[] content = row.getBinary(1);
        final String format = row.isNullAt(2) ? null : row.getString(2);
        if ("JSON".equalsIgnoreCase(format)) {
            prettyPrintJson(content, outputStream);
        } else if ("XML".equalsIgnoreCase(format)) {
            prettyPrintXml(content, outputStream);
        } else {
            if (this.encoding != null) {
                outputStream.write(new String(content).getBytes(this.encoding));
            } else {
                outputStream.write(content);
            }
        }
    }

    private void prettyPrintJson(byte[] content, OutputStream outputStream) throws IOException {
        JsonNode node = this.objectMapper.readTree(content);
        String prettyJson = node.toPrettyString();
        if (this.encoding != null) {
            outputStream.write(prettyJson.getBytes(this.encoding));
        } else {
            outputStream.write(prettyJson.getBytes());
        }
    }

    private void prettyPrintXml(byte[] content, OutputStream outputStream) {
        Result result = new StreamResult(outputStream);
        Source source = new StreamSource(new ByteArrayInputStream(content));
        try {
            this.transformer.transform(source, result);
        } catch (TransformerException e) {
            throw new ConnectorException(String.format("Unable to pretty print XML; cause: %s", e.getMessage()), e);
        }
    }
}
