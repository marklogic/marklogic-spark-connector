/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

class GzipFileWriter extends DocumentFileWriter {

    GzipFileWriter(Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        super(properties, hadoopConfiguration);
    }

    @Override
    protected Path makeFilePath(String path, String documentUri) {
        return FileUtil.makePathFromDocumentURI(path, documentUri + ".gz");
    }

    @Override
    protected OutputStream makeOutputStream(Path path) throws IOException {
        return new GZIPOutputStream(super.makeOutputStream(path));
    }
}
