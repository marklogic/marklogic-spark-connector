/*
 * Copyright © 2025 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Knows how to build a document from a row corresponding to our {@code DocumentRowSchema}.
 */
public class DocumentRowConverter implements RowConverter {

    private static final String CHUNKS_CLASSIFED_TEXT_COLUMN_NAME = "chunkClassifications";

    private final StructType schema;
    private final ObjectMapper objectMapper;
    private final String uriTemplate;
    private final Format documentFormat;
    private final boolean isStreamingFromFiles;

    public DocumentRowConverter(WriteContext writeContext) {
        this.schema = writeContext.getSchema();
        this.uriTemplate = writeContext.getStringOption(Options.WRITE_URI_TEMPLATE);
        this.documentFormat = writeContext.getDocumentFormat();
        this.objectMapper = new ObjectMapper();
        this.isStreamingFromFiles = writeContext.isStreamingFiles();
    }

    @Override
    public Iterator<DocBuilder.DocumentInputs> convertRow(InternalRow row) {
        final String uri = row.getString(0);

        final boolean isNakedProperties = row.isNullAt(1);
        if (isNakedProperties) {
            DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
            return Stream.of(new DocBuilder.DocumentInputs(uri, null, null, metadata)).iterator();
        }

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

        DocBuilder.DocumentInputs documentInputs = new DocBuilder.DocumentInputs(uri, bytesHandle, uriTemplateValues, documentRow.getMetadata());

        // These will all go away soon, their processing moved to DocumentProcessor.
        documentInputs.setClassificationResponse(documentRow.getClassificationResponse());
        documentInputs.setChunks(documentRow.getChunks());
        documentInputs.setClassifications(documentRow.getClassifications(CHUNKS_CLASSIFED_TEXT_COLUMN_NAME));

        return Stream.of(documentInputs).iterator();
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
