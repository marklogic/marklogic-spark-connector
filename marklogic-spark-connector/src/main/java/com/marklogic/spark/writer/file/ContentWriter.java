/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.dom.DOMHelper;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;

import javax.validation.constraints.NotNull;
import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Objects;

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

    private final boolean isStreamingFiles;
    // Only used when streaming.
    private final GenericDocumentManager documentManager;

    ContentWriter(Map<String, String> properties) {
        ContextSupport context = new ContextSupport(properties);
        this.encoding = determineEncoding(context);
        this.prettyPrint = "true".equalsIgnoreCase(context.getStringOption(Options.WRITE_FILES_PRETTY_PRINT));
        if (this.prettyPrint) {
            this.objectMapper = new ObjectMapper();
            this.transformer = newTransformer();
        } else {
            this.transformer = null;
            this.objectMapper = null;
        }

        this.isStreamingFiles = context.isStreamingFiles();
        if (this.isStreamingFiles) {
            this.documentManager = context.connectToMarkLogic().newDocumentManager();
            if (context.hasOption(Options.READ_DOCUMENTS_CATEGORIES)) {
                this.documentManager.setMetadataCategories(ContextSupport.getRequestedMetadata(context));
            }
        } else {
            this.documentManager = null;
        }
    }

    void writeContent(InternalRow row, @NotNull OutputStream outputStream) throws IOException {
        if (this.isStreamingFiles) {
            streamDocumentToFile(row, outputStream);
        } else if (this.prettyPrint) {
            prettyPrintContent(row, outputStream);
        } else {
            byte[] binary = row.getBinary(1);
            Objects.requireNonNull(binary);
            if (this.encoding != null) {
                // We know the string from MarkLogic is UTF-8, so we use getBytes to convert it to the user's
                // specified encoding (as opposed to new String(bytes, encoding)).
                outputStream.write(new String(binary).getBytes(this.encoding));
            } else {
                outputStream.write(binary);
            }
        }
    }

    void writeMetadata(InternalRow row, OutputStream outputStream) throws IOException {
        String metadataXml = DocumentRowSchema.makeDocumentMetadata(row).toString();
        writeMetadata(metadataXml, outputStream);
    }

    /**
     * When streaming documents to an archive, the metadata unfortunately has to be retrieved in a separate request
     * per document. This is due to the Java Client hardcoding "content" as a category in a POST to v1/search. A
     * future fix to the Java Client to not hardcode this will allow for the metadata to be retrieved during the
     * reader phase.
     */
    void writeMetadataWhileStreaming(String documentUri, OutputStream outputStream) throws IOException {
        DocumentMetadataHandle metadata = this.documentManager.readMetadata(documentUri, new DocumentMetadataHandle());
        writeMetadata(metadata.toString(), outputStream);
    }

    private void writeMetadata(String metadataXml, OutputStream outputStream) throws IOException {
        // Must honor the encoding here as well, as a user could easily have values that require encoding in metadata
        // values or in a properties fragment.
        if (this.encoding != null) {
            outputStream.write(metadataXml.getBytes(this.encoding));
        } else {
            outputStream.write(metadataXml.getBytes());
        }
    }

    private Charset determineEncoding(ContextSupport context) {
        if (context.hasOption(Options.WRITE_FILES_ENCODING)) {
            String encodingValue = context.getStringOption(Options.WRITE_FILES_ENCODING);
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
            final Transformer t = DOMHelper.newTransformerFactory().newTransformer();
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

    private void prettyPrintContent(InternalRow row, @NotNull OutputStream outputStream) throws IOException {
        final byte[] content = row.getBinary(1);
        Objects.requireNonNull(content);
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

    private void streamDocumentToFile(InternalRow row, OutputStream outputStream) throws IOException {
        String uri = row.getString(0);
        InputStream inputStream = documentManager.read(uri, new InputStreamHandle()).get();
        // commons-io is a dependency of Spark and a common utility for copying between two steams.
        IOUtils.copy(inputStream, outputStream);
    }
}
