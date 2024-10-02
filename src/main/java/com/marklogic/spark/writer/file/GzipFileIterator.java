/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.reader.file.GzipFileReader;
import com.marklogic.spark.writer.DocBuilder;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Exists solely to provide an implementation of {@code Closeable} so that the {@code GzipFileReader} can be closed
 * after the corresponding document is written to MarkLogic.
 */
public class GzipFileIterator implements Iterator<DocBuilder.DocumentInputs>, Closeable {

    private final GzipFileReader gzipFileReader;
    private Iterator<DocBuilder.DocumentInputs> iterator;

    public GzipFileIterator(GzipFileReader reader, Format documentFormat) {
        this.gzipFileReader = reader;
        reader.next();
        String uri = reader.get().getString(0);
        InputStreamHandle contentHandle = reader.getStreamingContentHandle();
        if (documentFormat != null) {
            contentHandle.withFormat(documentFormat);
        }
        this.iterator = Stream.of(new DocBuilder.DocumentInputs(uri, contentHandle, null, null)).iterator();
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public DocBuilder.DocumentInputs next() {
        return this.iterator.next();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(gzipFileReader);
    }
}
