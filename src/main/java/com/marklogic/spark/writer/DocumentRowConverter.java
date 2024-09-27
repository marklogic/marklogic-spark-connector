/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

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
import com.marklogic.spark.reader.file.ArchiveFileReader;
import com.marklogic.spark.reader.file.FileContext;
import com.marklogic.spark.reader.file.FilePartition;
import com.marklogic.spark.writer.file.ArchiveFileIterator;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Knows how to build a document from a row corresponding to our {@code DocumentRowSchema}.
 */
class DocumentRowConverter implements RowConverter {

    private final ObjectMapper objectMapper;
    private final String uriTemplate;
    private final Format documentFormat;
    private final boolean isStreamingFromFiles;

    DocumentRowConverter(WriteContext writeContext) {
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

        return this.isStreamingFromFiles ? readContentFromFile(uri, row) : readContentFromRow(uri, row);
    }

    @Override
    public List<DocBuilder.DocumentInputs> getRemainingDocumentInputs() {
        return new ArrayList<>();
    }

    private Iterator<DocBuilder.DocumentInputs> readContentFromRow(String uri, InternalRow row) {
        BytesHandle bytesHandle = new BytesHandle(row.getBinary(1));
        if (this.documentFormat != null) {
            bytesHandle.withFormat(this.documentFormat);
        }
        JsonNode uriTemplateValues = null;
        if (this.uriTemplate != null && this.uriTemplate.trim().length() > 0) {
            String format = row.isNullAt(2) ? null : row.getString(2);
            uriTemplateValues = deserializeContentToJson(uri, bytesHandle, format);
        }
        DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
        return Stream.of(new DocBuilder.DocumentInputs(uri, bytesHandle, uriTemplateValues, metadata)).iterator();
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
    private Iterator<DocBuilder.DocumentInputs> readContentFromFile(String filePath, InternalRow row) {
        byte[] bytes = row.getBinary(1);
        FileContext fileContext;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            fileContext = (FileContext) ois.readObject();
        } catch (Exception e) {
            throw new ConnectorException(String.format("Unable to read from file %s; cause: %s", filePath, e.getMessage()));
        }

        if ("archive".equalsIgnoreCase(fileContext.getStringOption(Options.READ_FILES_TYPE))) {
            return buildIteratorForArchiveFile(filePath, fileContext);
        }

        final String decodedPath = fileContext.decodeFilePath(filePath);
        InputStreamHandle streamHandle = new InputStreamHandle(fileContext.openFile(decodedPath));
        if (this.documentFormat != null) {
            streamHandle.withFormat(this.documentFormat);
        }
        DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
        return Stream.of(new DocBuilder.DocumentInputs(filePath, streamHandle, null, metadata)).iterator();
    }

    private Iterator<DocBuilder.DocumentInputs> buildIteratorForArchiveFile(String filePath, FileContext fileContext) {
        FilePartition filePartition = new FilePartition(Arrays.asList(filePath));
        ArchiveFileReader archiveFileReader = new ArchiveFileReader(
            filePartition, fileContext, ArchiveFileReader.StreamingMode.STREAM_DURING_WRITER_PHASE
        );
        return new ArchiveFileIterator(archiveFileReader, this.documentFormat);
    }
}
