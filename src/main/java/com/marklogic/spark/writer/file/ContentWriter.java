package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.InternalRow;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
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

    ContentWriter(Map<String, String> properties) {
        this.prettyPrint = "true".equalsIgnoreCase(properties.get(Options.WRITE_FILES_PRETTY_PRINT));
        if (this.prettyPrint) {
            this.objectMapper = new ObjectMapper();
            try {
                this.transformer = TransformerFactory.newInstance().newTransformer();
            } catch (TransformerConfigurationException e) {
                throw new ConnectorException(
                    String.format("Unable to instantiate transformer for pretty-printing XML; cause: %s", e.getMessage()), e
                );
            }
            this.transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            this.transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            this.transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        } else {
            this.transformer = null;
            this.objectMapper = null;
        }
    }

    void writeContent(InternalRow row, OutputStream outputStream) throws IOException {
        if (this.prettyPrint) {
            prettyPrintContent(row, outputStream);
        } else {
            outputStream.write(row.getBinary(1));
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
            outputStream.write(content);
        }
    }

    private void prettyPrintJson(byte[] content, OutputStream outputStream) throws IOException {
        JsonNode node = this.objectMapper.readTree(content);
        outputStream.write(node.toPrettyString().getBytes());
    }

    private void prettyPrintXml(byte[] content, OutputStream outputStream) {
        Result result = new StreamResult(outputStream);
        Source source = new StreamSource(new ByteArrayInputStream(content));
        try {
            this.transformer.transform(source, result);
        } catch (TransformerException e) {
            throw new RuntimeException(e);
        }
    }
}
