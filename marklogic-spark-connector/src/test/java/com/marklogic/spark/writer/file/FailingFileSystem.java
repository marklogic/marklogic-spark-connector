/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Local file system registered under the "failing" scheme whose output streams can fail when closed, simulating
 * e.g. a failure to complete an upload to an object store.
 */
public class FailingFileSystem extends RawLocalFileSystem {

    public static final String SCHEME = "failing";
    public static final String FAILURE_MESSAGE = "Simulated failure on close";

    public static volatile boolean failOnClose;

    public static void register(Configuration config) {
        config.set("fs." + SCHEME + ".impl", FailingFileSystem.class.getName());
        config.setBoolean("fs." + SCHEME + ".impl.disable.cache", true);
    }

    public static String toPath(java.nio.file.Path dir) {
        return SCHEME + "://" + dir.toAbsolutePath();
    }

    @Override
    public URI getUri() {
        return URI.create(SCHEME + ":///");
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FSDataOutputStream create(Path path, boolean overwrite, int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
        OutputStream out = super.create(path, overwrite, bufferSize, replication, blockSize, progress);
        return new FSDataOutputStream(new FilterOutputStream(out) {
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                super.close();
                if (failOnClose) {
                    throw new IOException(FAILURE_MESSAGE);
                }
            }
        }, statistics);
    }
}
