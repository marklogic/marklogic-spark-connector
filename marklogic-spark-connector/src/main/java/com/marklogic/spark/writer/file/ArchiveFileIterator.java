/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.Util;
import com.marklogic.spark.core.DocumentInputs;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import com.marklogic.spark.reader.file.ArchiveFileReader;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Provides an {@code Iterator} interface on top of an {@code ArchiveFileReader}, thereby allowing a
 * {@code DocumentRowConverter} to build sets of document inputs from an archive file without reading any content entry
 * into memory - thus supporting streaming of an archive.
 */
public class ArchiveFileIterator implements Iterator<DocumentInputs>, Closeable {

    private final ArchiveFileReader archiveFileReader;
    private final Format documentFormat;

    public ArchiveFileIterator(ArchiveFileReader archiveFileReader, Format documentFormat) {
        this.archiveFileReader = archiveFileReader;
        this.documentFormat = documentFormat;
    }

    @Override
    public boolean hasNext() {
        return archiveFileReader.next();
    }

    @Override
    // Suppressing sonar warning about throwing a NoSuchElementException. We know this is only used by
    // DocumentRowConverter, which properly calls hasNext() before calling next().
    @SuppressWarnings("java:S2272")
    public DocumentInputs next() {
        InternalRow row = archiveFileReader.get();
        String uri = row.getString(0);
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Creating input stream for entry {}", uri);
        }
        InputStreamHandle contentHandle = archiveFileReader.getContentHandleForCurrentZipEntry();
        DocumentMetadataHandle metadata = DocumentRowSchema.makeDocumentMetadata(row);
        if (this.documentFormat != null) {
            contentHandle.withFormat(this.documentFormat);
        }
        return new DocumentInputs(uri, contentHandle, null, metadata);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(archiveFileReader);
    }
}
