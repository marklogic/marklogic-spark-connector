/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.writer.DocBuilder;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Exists solely to provide an implementation of {@code Closeable} so that the {@code InputStreamHandle} can be closed
 * after the corresponding document is written to MarkLogic.
 */
public class FileIterator implements Iterator<DocBuilder.DocumentInputs>, Closeable {

    private final InputStreamHandle contentHandle;
    private final Iterator<DocBuilder.DocumentInputs> iterator;

    public FileIterator(InputStreamHandle contentHandle, DocBuilder.DocumentInputs inputs) {
        this.contentHandle = contentHandle;
        this.iterator = Stream.of(inputs).iterator();
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
        IOUtils.closeQuietly(contentHandle);
    }
}
