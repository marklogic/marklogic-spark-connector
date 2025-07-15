/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.spark.core.DocumentInputs;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Exists solely to provide an implementation of {@code Closeable} so that the {@code InputStreamHandle} can be closed
 * after the corresponding document is written to MarkLogic.
 */
public class FileIterator implements Iterator<DocumentInputs>, Closeable {

    private final InputStreamHandle contentHandle;
    private final Iterator<DocumentInputs> iterator;

    public FileIterator(InputStreamHandle contentHandle, DocumentInputs inputs) {
        this.contentHandle = contentHandle;
        this.iterator = Stream.of(inputs).iterator();
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public DocumentInputs next() {
        return this.iterator.next();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(contentHandle);
    }
}
