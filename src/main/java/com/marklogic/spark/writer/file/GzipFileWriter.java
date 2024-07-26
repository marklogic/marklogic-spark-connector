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
    protected String makeFilePath(String uri) {
        return super.makeFilePath(uri) + ".gz";
    }

    @Override
    protected OutputStream makeOutputStream(Path path) throws IOException {
        return new GZIPOutputStream(super.makeOutputStream(path));
    }
}
