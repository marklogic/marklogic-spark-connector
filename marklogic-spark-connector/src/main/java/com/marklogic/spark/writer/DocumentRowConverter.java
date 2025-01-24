/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.marklogic.client.io.*;
import com.marklogic.langchain4j.classifier.TextClassifier;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import com.marklogic.spark.langchain4j.TextClassifierFactory;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.*;
import com.marklogic.spark.writer.file.ArchiveFileIterator;
import com.marklogic.spark.writer.file.FileIterator;
import com.marklogic.spark.writer.file.GzipFileIterator;
import com.marklogic.spark.writer.file.ZipFileIterator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Knows how to build a document from a row corresponding to our {@code DocumentRowSchema}.
 */
class DocumentRowConverter implements RowConverter {

    private final ObjectMapper objectMapper;
    private final String uriTemplate;
    private final Format documentFormat;
    private final boolean isStreamingFromFiles;
    private final TextClassifier textClassifier;
    private final DocumentBuilderFactory documentBuilderFactory;
    private final XmlMapper xmlMapper;
    private final Transformer transformer;


    DocumentRowConverter(WriteContext writeContext) {
        this.uriTemplate = writeContext.getStringOption(Options.WRITE_URI_TEMPLATE);
        this.documentFormat = writeContext.getDocumentFormat();
        this.objectMapper = new ObjectMapper();
        this.isStreamingFromFiles = writeContext.isStreamingFiles();
        textClassifier = TextClassifierFactory.makeClassifier(writeContext);
        if (textClassifier != null) {
            documentBuilderFactory = DocumentBuilderFactory.newInstance();
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
            try {
                transformer = transformerFactory.newTransformer();
            } catch (TransformerConfigurationException e) {
                throw new ConnectorException(String.format("Unable to initialize Classifier: cause: %s", e.getMessage()), e);
            }
            xmlMapper = new XmlMapper();
        } else {
            documentBuilderFactory = null;
            transformer = null;
            xmlMapper = null;
        }
    }

    @Override
    public Iterator<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        final String uri = row.getString(0);

        final boolean isNakedProperties = row.isNullAt(1);
        if (isNakedProperties) {
            DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
            return Stream.of(new DocBuilder.DocumentInputs(uri, null, null, metadata)).iterator();
        }

        // I think it'll be fine to not support text extraction when streaming. Streaming is typically for very large
        // files and ironically is usually slower because documents have to be written one at a time.
        return this.isStreamingFromFiles ? streamContentFromFile(uri, row) : readContentFromRow(uri, row);
    }

    @Override
    public Iterator<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return Stream.<DocBuilder.DocumentInputs>empty().iterator();
    }

    private Iterator<DocBuilder.DocumentInputs> readContentFromRow(String uri, InternalRow row) {
        BytesHandle bytesHandle = new BytesHandle(row.getBinary(1));
        setHandleFormat(bytesHandle, row);

        JsonNode uriTemplateValues = null;
        if (this.uriTemplate != null && this.uriTemplate.trim().length() > 0) {
            String format = row.isNullAt(2) ? null : row.getString(2);
            uriTemplateValues = deserializeContentToJson(uri, bytesHandle, format);
        }

        DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
        DocBuilder.DocumentInputs mainInputs = new DocBuilder.DocumentInputs(uri, bytesHandle, uriTemplateValues, metadata);
        final boolean extractedTextExists = row.numFields() == 9;
        if (extractedTextExists) {
            // Very primitive for now.
            final String extractedText = row.getString(8);

            String textUri = uri + "-extracted-text.txt";
            return Stream.of(mainInputs, new DocBuilder.DocumentInputs(textUri, new StringHandle(extractedText), uriTemplateValues, metadata)).iterator();
        }

        if ((textClassifier != null) && !row.isNullAt(2)) {
            String classificationText = new String(bytesHandle.get(), StandardCharsets.UTF_8);
            String format = row.getString(2);
            if ("XML".equals(format)) {
                mainInputs = appendDocumentClassificationToXmlContent(uri, classificationText, uriTemplateValues, metadata);
            } else if ("JSON".equals(format)) {
                mainInputs = appendDocumentClassificationToJsonContent(uri, classificationText, uriTemplateValues, metadata);
            }
        }

        return Stream.of(mainInputs).iterator();
    }

    private DocBuilder.DocumentInputs appendDocumentClassificationToJsonContent(
        String uri, String classificationText, JsonNode uriTemplateValues, DocumentMetadataHandle metadata
    ) {
        byte[] response = textClassifier.classifyTextToBytes(uri, classificationText);
        try {
            ObjectNode originalJson = (ObjectNode) objectMapper.readTree(classificationText);
            JsonNode structuredDocumentNode = xmlMapper.readTree(response).get("STRUCTUREDDOCUMENT");
            originalJson.set("classification", structuredDocumentNode);
            return new DocBuilder.DocumentInputs(uri, new JacksonHandle(originalJson), uriTemplateValues, metadata);
        } catch (IOException e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
        }
    }

    public DocBuilder.DocumentInputs appendDocumentClassificationToXmlContent(
        String uri, String classificationText, JsonNode uriTemplateValues, DocumentMetadataHandle metadata
    ) {
        String DEFAULT_XML_NAMESPACE = "http://marklogic.com/appservices/model";
        Document responseDoc = textClassifier.classifyTextToXml(uri, classificationText);
        try {
            DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
            Document originalDoc = builder.parse(new InputSource(new StringReader(classificationText)));

            NodeList structuredDocumentNodeChildNodes = responseDoc.getElementsByTagName("STRUCTUREDDOCUMENT").item(0).getChildNodes();
            Node classificationNode = originalDoc.createElementNS(DEFAULT_XML_NAMESPACE,"classification");
            for (int i = 0; i < structuredDocumentNodeChildNodes.getLength(); i++) {
                Node importedChildNode = originalDoc.importNode(structuredDocumentNodeChildNodes.item(i),true);
                classificationNode.appendChild(importedChildNode);
            }
            originalDoc.getFirstChild().appendChild(classificationNode);

            BytesHandle newBytesHandle = new BytesHandle(convertDomToByteArray(originalDoc));
            return new DocBuilder.DocumentInputs(uri, newBytesHandle, uriTemplateValues, metadata);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
        }
    }

    public byte[] convertDomToByteArray(Document doc) throws TransformerException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        transformer.transform(new DOMSource(doc), new StreamResult(baos));
        return baos.toByteArray();
    }

    /**
     * Setting the format now to assist with operations that occur before the document is written to MarkLogic.
     */
    private void setHandleFormat(BytesHandle bytesHandle, InternalRow row) {
        if (this.documentFormat != null) {
            bytesHandle.withFormat(this.documentFormat);
        } else if (!row.isNullAt(2)) {
            String format = row.getString(2);
            try {
                bytesHandle.withFormat(Format.valueOf(format.toUpperCase()));
            } catch (IllegalArgumentException e) {
                // We don't ever expect this to happen, but in case it does - we'll proceed with a null format
                // on the handle, as it's not essential that it be set.
                if (Util.MAIN_LOGGER.isDebugEnabled()) {
                    Util.MAIN_LOGGER.debug("Unable to set format on row with URI: {}; format: {}; error: {}",
                        row.getString(0), format, e.getMessage());
                }
            }
        }
    }

    private JsonNode deserializeContentToJson(String initialUri, BytesHandle contentHandle, String format) {
        try {
            return objectMapper.readTree(contentHandle.get());
        } catch (IOException e) {
            // Preserves the initial support in the 2.2.0 release.
            ObjectNode values = objectMapper.createObjectNode();
            values.put("URI", initialUri);
            if (format != null) {
                values.put("format", format);
            }
            return values;
        }
    }

    /**
     * In a scenario where the user wants to stream a file into MarkLogic, the content column will contain a serialized
     * instance of {@code FileContext}, which is used to stream the file into a {@code InputStreamHandle}.
     */
    private Iterator<DocBuilder.DocumentInputs> streamContentFromFile(String filePath, InternalRow row) {
        byte[] bytes = row.getBinary(1);
        FileContext fileContext;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            fileContext = (FileContext) ois.readObject();
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read from file %s; cause: %s", filePath, e.getMessage()));
        }

        if ("archive".equalsIgnoreCase(fileContext.getStringOption(Options.READ_FILES_TYPE))) {
            return buildIteratorForArchiveFile(filePath, fileContext);
        } else if (fileContext.isZip()) {
            return buildIteratorForZipFile(filePath, fileContext);
        } else if (fileContext.isGzip()) {
            return buildIteratorForGzipFile(filePath, fileContext);
        }
        return buildIteratorForGenericFile(row, filePath, fileContext);
    }

    private Iterator<DocBuilder.DocumentInputs> buildIteratorForGenericFile(InternalRow row, String filePath, FileContext fileContext) {
        InputStreamHandle contentHandle = new InputStreamHandle(fileContext.openFile(filePath));
        if (this.documentFormat != null) {
            contentHandle.withFormat(this.documentFormat);
        }
        DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
        return new FileIterator(contentHandle, new DocBuilder.DocumentInputs(filePath, contentHandle, null, metadata));
    }

    private Iterator<DocBuilder.DocumentInputs> buildIteratorForArchiveFile(String filePath, FileContext fileContext) {
        FilePartition filePartition = new FilePartition(filePath);
        ArchiveFileReader archiveFileReader = new ArchiveFileReader(
            filePartition, fileContext, StreamingMode.STREAM_DURING_WRITER_PHASE
        );
        return new ArchiveFileIterator(archiveFileReader, this.documentFormat);
    }

    private Iterator<DocBuilder.DocumentInputs> buildIteratorForZipFile(String filePath, FileContext fileContext) {
        FilePartition filePartition = new FilePartition(filePath);
        ZipFileReader reader = new ZipFileReader(filePartition, fileContext, StreamingMode.STREAM_DURING_WRITER_PHASE);
        return new ZipFileIterator(reader, this.documentFormat);
    }

    private Iterator<DocBuilder.DocumentInputs> buildIteratorForGzipFile(String filePath, FileContext fileContext) {
        GzipFileReader reader = new GzipFileReader(new FilePartition(filePath), fileContext, StreamingMode.STREAM_DURING_WRITER_PHASE);
        return new GzipFileIterator(reader, this.documentFormat);
    }
}
