/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.*;
import com.marklogic.langchain4j.dom.DOMHelper;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.langchain4j.NamespaceContextFactory;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.*;
import com.marklogic.spark.writer.DocBuilder;
import com.marklogic.spark.writer.RowConverter;
import com.marklogic.spark.writer.WriteContext;
import com.marklogic.spark.writer.file.ArchiveFileIterator;
import com.marklogic.spark.writer.file.FileIterator;
import com.marklogic.spark.writer.file.GzipFileIterator;
import com.marklogic.spark.writer.file.ZipFileIterator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Knows how to build a document from a row corresponding to our {@code DocumentRowSchema}.
 */
public class DocumentRowConverter implements RowConverter {

    private static final String CLASSIFICATION_MAIN_ELEMENT = "STRUCTUREDDOCUMENT";
    private static final String DEFAULT_XML_NAMESPACE = "http://marklogic.com/appservices/model";

    private final StructType schema;
    private final ObjectMapper objectMapper;
    private final DOMHelper domHelper;
    private final String uriTemplate;
    private final Format documentFormat;
    private final Format extractedTextFormat;
    private final boolean isStreamingFromFiles;
    private DocumentBuilderFactory documentBuilderFactory = null;

    public DocumentRowConverter(WriteContext writeContext) {
        this.schema = writeContext.getSchema();
        this.uriTemplate = writeContext.getStringOption(Options.WRITE_URI_TEMPLATE);
        this.documentFormat = writeContext.getDocumentFormat();
        this.objectMapper = new ObjectMapper();
        this.domHelper = new DOMHelper(NamespaceContextFactory.makeDefaultNamespaceContext());
        this.isStreamingFromFiles = writeContext.isStreamingFiles();
        this.extractedTextFormat = "xml".equalsIgnoreCase(writeContext.getStringOption(Options.WRITE_EXTRACTED_TEXT_FORMAT)) ?
            Format.XML : Format.JSON;
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
        DocumentRow documentRow = new DocumentRow(row, this.schema);
        BytesHandle bytesHandle = documentRow.getContent(this.documentFormat);

        JsonNode uriTemplateValues = null;
        if (this.uriTemplate != null && this.uriTemplate.trim().length() > 0) {
            String format = documentRow.getFormat();
            uriTemplateValues = deserializeContentToJson(uri, bytesHandle, format);
        }
        DocumentMetadataHandle metadata = documentRow.getMetadata();

        List<DocBuilder.DocumentInputs> documentsToReturn = new ArrayList<>();

        Optional<String> extractedText = documentRow.getExtractedText();
        Optional<byte[]> classificationResponse = documentRow.getClassificationResponse();
        if (classificationResponse.isPresent() && extractedText.isEmpty()) {
            String format = row.getString(2);
            if ("XML".equals(format)) {
                documentsToReturn.add(
                    appendDocumentClassificationToXmlContent(row.getBinary(1), uri, classificationResponse.get(), uriTemplateValues, metadata)
                );
            }
        } else {
            documentsToReturn.add(new DocBuilder.DocumentInputs(uri, bytesHandle, uriTemplateValues, metadata));
        }

        if (extractedText.isPresent()) {
            DocBuilder.DocumentInputs extractedTextDocument = Format.XML.equals(this.extractedTextFormat) ?
                    buildExtractedXmlDocument(uri, extractedText.get(), metadata) :
                    buildExtractedJsonDocument(uri, extractedText.get(), metadata);
            documentsToReturn.add(extractedTextDocument);
        }

        return documentsToReturn.iterator();
    }

    public DocBuilder.DocumentInputs appendDocumentClassificationToXmlContent(
        byte[] originalContent, String uri, byte[] classificationResponse, JsonNode uriTemplateValues, DocumentMetadataHandle metadata
    ) {
        if (documentBuilderFactory == null) {
            documentBuilderFactory = DocumentBuilderFactory.newInstance();
        }
        try {
            DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
            Document originalDoc = builder.parse(new ByteArrayInputStream(originalContent));

            Document responseDoc = builder.parse(new ByteArrayInputStream(classificationResponse));
            NodeList structuredDocumentNodeChildNodes = responseDoc.getElementsByTagName(CLASSIFICATION_MAIN_ELEMENT).item(0).getChildNodes();
            Node classificationNode = originalDoc.createElementNS(DEFAULT_XML_NAMESPACE,"classification");
            for (int i = 0; i < structuredDocumentNodeChildNodes.getLength(); i++) {
                Node importedChildNode = originalDoc.importNode(structuredDocumentNodeChildNodes.item(i),true);
                classificationNode.appendChild(importedChildNode);
            }
            originalDoc.getFirstChild().appendChild(classificationNode);

            return new DocBuilder.DocumentInputs(uri, new DOMHandle(originalDoc), uriTemplateValues, metadata);
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to classify data from document with URI: %s; cause: %s", uri, e.getMessage()), e);
        }
    }

    private DocBuilder.DocumentInputs buildExtractedJsonDocument(String sourceUri, String extractedText, DocumentMetadataHandle metadata) {
        ObjectNode doc = objectMapper.createObjectNode();
        doc.put("content", extractedText);
        // No uriTemplateValues as there's nothing to pull out of the binary document.
        // For metadata - we'll reuse the binary document metadata for now.
        return new DocBuilder.DocumentInputs(sourceUri + "-extracted-text.json", new JacksonHandle(doc), null, metadata);
    }

    private DocBuilder.DocumentInputs buildExtractedXmlDocument(String sourceUri, String extractedText, DocumentMetadataHandle metadata) {
        Document doc = domHelper.newDocument();
        Element root = doc.createElementNS(com.marklogic.langchain4j.Util.DEFAULT_XML_NAMESPACE, "root");
        doc.appendChild(root);
        Element content = doc.createElementNS(com.marklogic.langchain4j.Util.DEFAULT_XML_NAMESPACE, "content");
        content.appendChild(doc.createTextNode(extractedText));
        root.appendChild(content);
        return new DocBuilder.DocumentInputs(sourceUri + "-extracted-text.xml", new DOMHandle(doc), null, metadata);
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
