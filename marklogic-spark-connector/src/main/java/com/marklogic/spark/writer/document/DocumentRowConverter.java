/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.*;
import com.marklogic.spark.writer.RowConverter;
import com.marklogic.spark.writer.WriteContext;
import com.marklogic.spark.writer.file.ArchiveFileIterator;
import com.marklogic.spark.writer.file.FileIterator;
import com.marklogic.spark.writer.file.GzipFileIterator;
import com.marklogic.spark.writer.file.ZipFileIterator;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Knows how to build a document from a row corresponding to our {@code DocumentRowSchema}.
 */
public class DocumentRowConverter implements RowConverter {

    private final ObjectMapper objectMapper;
    private final String uriTemplate;
    private final Format documentFormat;
    private final boolean isStreamingFromFiles;

    public DocumentRowConverter(WriteContext writeContext) {
        this.uriTemplate = writeContext.getStringOption(Options.WRITE_URI_TEMPLATE);
        this.documentFormat = writeContext.getDocumentFormat();
        this.objectMapper = new ObjectMapper();
        this.isStreamingFromFiles = writeContext.isStreamingFiles();
    }

    @Override
    public Iterator<DocumentInputs> convertRow(InternalRow row) {
        final String uri = row.getString(0);

        final boolean isNakedProperties = row.isNullAt(1);
        if (isNakedProperties) {
            DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
            return Stream.of(new DocumentInputs(uri, null, null, metadata)).iterator();
        }

        return this.isStreamingFromFiles ? streamContentFromFile(uri, row) : readContentFromRow(uri, row);
    }

    @Override
    public Iterator<DocumentInputs> getRemainingDocumentInputs() {
        return Stream.<DocumentInputs>empty().iterator();
    }

    private Iterator<DocumentInputs> readContentFromRow(String uri, InternalRow row) {
        DocumentRow documentRow = new DocumentRow(row);
        BytesHandle bytesHandle = documentRow.getContent(this.documentFormat);

        JsonNode uriTemplateValues = null;
        if (this.uriTemplate != null && !this.uriTemplate.trim().isEmpty()) {
            String format = documentRow.getFormat();
            uriTemplateValues = deserializeContentToJson(uri, bytesHandle, format);
        }

        DocumentInputs documentInputs = new DocumentInputs(uri, bytesHandle, uriTemplateValues, documentRow.getMetadata());
        return Stream.of(documentInputs).iterator();
    }

    private JsonNode deserializeContentToJson(String initialUri, BytesHandle contentHandle, String format) {
        // Added this check in release 3.0.0 to avoid unnecessary deserialization attempts. We can only assume this when
        // the format is XML, as binary/text formats may still be deserializable to JSON.
        if ("xml".equalsIgnoreCase(format) || Format.XML.equals(contentHandle.getFormat())) {
            return buildDefaultUriTemplateValues(initialUri, format);
        }

        try {
            return objectMapper.readTree(contentHandle.get());
        } catch (IOException e) {
            // Because we can't reliably know if the content is JSON or not, we don't want to log this error at info
            // or warn because it may be innocuous. For example, a file may not have an extension and not be JSON, and
            // we can't know that until we try to deserialize it. A deserialization failure is expected in that case.
            // But there could be a legitimate issue if the content is supposed to be JSON and is valid, but cannot be
            // deserialized by Jackson. This occurred in testing with a document containing a very large string value
            // that exceeded a Jackson limit. So we need visibility to the error, we just do it at the debug level so
            // it's possible to get that error without alarming users about what are usually innocuous failures.
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Unable to deserialize content for URI template values for document {}; cause: {}",
                    initialUri, e.getMessage());
            }

            return buildDefaultUriTemplateValues(initialUri, format);
        }
    }

    private ObjectNode buildDefaultUriTemplateValues(String initialUri, String format) {
        // Release 2.3.0 added support for using a URI template against JSON content. Prior to that, the only values
        // exposed to a URI template were "URI" and "format". So when the content is not JSON, we use this default set
        // of values.
        ObjectNode values = objectMapper.createObjectNode();
        values.put("URI", initialUri);
        if (format != null) {
            values.put("format", format);
        }
        return values;
    }

    /**
     * In a scenario where the user wants to stream a file into MarkLogic, the content column will contain a serialized
     * instance of {@code FileContext}, which is used to stream the file into a {@code InputStreamHandle}.
     */
    private Iterator<DocumentInputs> streamContentFromFile(String filePath, InternalRow row) {
        byte[] bytes = row.getBinary(1);
        Objects.requireNonNull(bytes);
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

    private Iterator<DocumentInputs> buildIteratorForGenericFile(InternalRow row, String filePath, FileContext fileContext) {
        InputStreamHandle contentHandle = new InputStreamHandle(fileContext.openFile(filePath));
        if (this.documentFormat != null) {
            contentHandle.withFormat(this.documentFormat);
        }
        DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
        return new FileIterator(contentHandle, new DocumentInputs(filePath, contentHandle, null, metadata));
    }

    private Iterator<DocumentInputs> buildIteratorForArchiveFile(String filePath, FileContext fileContext) {
        FilePartition filePartition = new FilePartition(filePath);
        ArchiveFileReader archiveFileReader = new ArchiveFileReader(
            filePartition, fileContext, StreamingMode.STREAM_DURING_WRITER_PHASE
        );
        return new ArchiveFileIterator(archiveFileReader, this.documentFormat);
    }

    private Iterator<DocumentInputs> buildIteratorForZipFile(String filePath, FileContext fileContext) {
        FilePartition filePartition = new FilePartition(filePath);
        ZipFileReader reader = new ZipFileReader(filePartition, fileContext, StreamingMode.STREAM_DURING_WRITER_PHASE);
        return new ZipFileIterator(reader, this.documentFormat);
    }

    private Iterator<DocumentInputs> buildIteratorForGzipFile(String filePath, FileContext fileContext) {
        GzipFileReader reader = new GzipFileReader(new FilePartition(filePath), fileContext, StreamingMode.STREAM_DURING_WRITER_PHASE);
        return new GzipFileIterator(reader, this.documentFormat);
    }
}
