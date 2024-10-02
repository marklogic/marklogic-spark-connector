/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.Util;
import com.marklogic.spark.reader.file.ZipFileReader;
import com.marklogic.spark.writer.DocBuilder;
import org.apache.commons.crypto.utils.IoUtils;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.Closeable;
import java.util.Iterator;

public class ZipFileIterator implements Iterator<DocBuilder.DocumentInputs>, Closeable {

    private final ZipFileReader zipFileReader;
    private final Format documentFormat;

    public ZipFileIterator(ZipFileReader zipFileReader, Format documentFormat) {
        this.zipFileReader = zipFileReader;
        this.documentFormat = documentFormat;
    }

    @Override
    public boolean hasNext() {
        return zipFileReader.next();
    }

    @Override
    // Suppressing sonar warning about throwing a NoSuchElementException. We know this is only used by
    // DocumentRowConverter, which properly calls hasNext() before calling next().
    @SuppressWarnings("java:S2272")
    public DocBuilder.DocumentInputs next() {
        InternalRow row = zipFileReader.get();
        String uri = row.getString(0);
        if (Util.MAIN_LOGGER.isDebugEnabled()) {
            Util.MAIN_LOGGER.debug("Creating input stream for entry {}", uri);
        }
        InputStreamHandle contentHandle = zipFileReader.getContentHandleForCurrentZipEntry();
        if (this.documentFormat != null) {
            contentHandle.withFormat(this.documentFormat);
        }
        return new DocBuilder.DocumentInputs(uri, contentHandle, null, null);
    }

    @Override
    public void close() {
        IoUtils.closeQuietly(zipFileReader);
    }
}
