/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An {@code InputStream} wrapper that throws a {@link ConnectorException} when more than
 * {@code maxBytes} bytes have been read. Intentionally does <em>not</em> propagate
 * {@code close()} to the underlying stream, so that the wrapped {@link java.util.zip.ZipInputStream}
 * remains open and can be advanced to subsequent zip entries after this wrapper is discarded.
 */
class MaxBytesInputStream extends FilterInputStream {

    private final long maxBytes;
    private long bytesRead = 0;

    MaxBytesInputStream(InputStream in, long maxBytes) {
        super(in);
        this.maxBytes = maxBytes;
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        if (b != -1) {
            checkLimit(1);
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = super.read(b, off, len);
        if (n > 0) {
            checkLimit(n);
        }
        return n;
    }

    /**
     * Overrides {@link FilterInputStream#skip} to route skipped bytes through {@link #read} so
     * the byte counter is kept up to date. The default {@code FilterInputStream.skip} delegates
     * directly to the underlying stream and would bypass limit enforcement.
     */
    @Override
    public long skip(long n) throws IOException {
        byte[] buffer = new byte[(int) Math.min(n, 4096L)];
        long remaining = n;
        long totalSkipped = 0;
        while (remaining > 0) {
            int toRead = (int) Math.min(remaining, buffer.length);
            int bytesRead = read(buffer, 0, toRead);
            if (bytesRead == -1) {
                break;
            }
            totalSkipped += bytesRead;
            remaining -= bytesRead;
        }
        return totalSkipped;
    }

    /**
     * Intentionally does not close the underlying stream. The caller (e.g. a zip-reading loop) retains
     * ownership of the {@link java.util.zip.ZipInputStream} and is responsible for closing it.
     */
    @Override
    public void close() {
        // Do not propagate close to the underlying ZipInputStream.
    }

    private void checkLimit(int n) {
        bytesRead += n;
        if (bytesRead > maxBytes) {
            throw new ConnectorException(String.format(
                "Zip entry uncompressed size exceeds the maximum of %d bytes. " +
                    "Use connector option '%s' to increase or disable this limit (set to -1 to disable).",
                maxBytes, Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES));
        }
    }
}
