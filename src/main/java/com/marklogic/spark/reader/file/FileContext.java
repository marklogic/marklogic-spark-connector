/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class FileContext extends ContextSupport implements Serializable {

    private SerializableConfiguration hadoopConfiguration;
    private final String encoding;

    public FileContext(Map<String, String> properties, SerializableConfiguration hadoopConfiguration) {
        super(properties);
        this.hadoopConfiguration = hadoopConfiguration;
        this.encoding = getStringOption(Options.READ_FILES_ENCODING);
        if (this.encoding != null) {
            try {
                Charset.forName(this.encoding);
            } catch (UnsupportedCharsetException e) {
                throw new ConnectorException(String.format("Unsupported encoding value: %s", this.encoding), e);
            }
        }
    }

    public boolean isZip() {
        return "zip".equalsIgnoreCase(getStringOption(Options.READ_FILES_COMPRESSION));
    }

    public boolean isGzip() {
        return "gzip".equalsIgnoreCase(getStringOption(Options.READ_FILES_COMPRESSION));
    }

    public InputStream openFile(String filePath) {
        return openFile(filePath, false);
    }

    public InputStream openFile(String filePath, boolean guessIfGzipped) {
        try {
            Path hadoopPath = new Path(filePath);
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            FSDataInputStream inputStream = fileSystem.open(hadoopPath);
            return isFileGzipped(filePath, guessIfGzipped) ? new GZIPInputStream(inputStream) : inputStream;
        } catch (Exception e) {
            throw new ConnectorException(String.format(
                "Unable to read file at %s; cause: %s", filePath, e.getMessage()), e);
        }
    }

    BufferedReader openFileReader(String filePath, boolean guessIfGzipped) {
        try {
            InputStream inputStream = openFile(filePath, guessIfGzipped);
            InputStreamReader inputStreamReader = this.encoding != null ?
                new InputStreamReader(inputStream, encoding) :
                new InputStreamReader(inputStream);
            return new BufferedReader(inputStreamReader);
        } catch (Exception e) {
            throw new ConnectorException(String.format(
                "Unable to read file at %s; cause: %s", filePath, e.getMessage()), e);
        }
    }

    public boolean isReadAbortOnFailure() {
        return getBooleanOption(Options.READ_FILES_ABORT_ON_FAILURE, true);
    }

    byte[] readBytes(InputStream inputStream) throws IOException {
        byte[] bytes = FileUtil.readBytes(inputStream);
        return this.encoding != null ? new String(bytes, this.encoding).getBytes() : bytes;
    }

    public String decodeFilePath(String path) {
        try {
            if (this.encoding != null) {
                return URLDecoder.decode(path, this.encoding);
            }
            return URLDecoder.decode(path, Charset.defaultCharset());
        } catch (UnsupportedEncodingException e) {
            if (Util.MAIN_LOGGER.isDebugEnabled()) {
                Util.MAIN_LOGGER.debug("Cannot decode path '{}', so will use path as-is. Error: {}", path, e.getMessage());
            }
            return path;
        }
    }

    private boolean isFileGzipped(String filePath, boolean guessIfGzipped) {
        if (this.isGzip()) {
            return true;
        }
        return guessIfGzipped && filePath != null && (filePath.endsWith(".gz") || filePath.endsWith(".gzip"));
    }
}
