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

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
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

    boolean isZip() {
        return "zip".equalsIgnoreCase(getStringOption(Options.READ_FILES_COMPRESSION));
    }

    boolean isGzip() {
        return "gzip".equalsIgnoreCase(getStringOption(Options.READ_FILES_COMPRESSION));
    }

    public InputStream openFile(String filePath) {
        try {
            Path hadoopPath = new Path(filePath);
            FileSystem fileSystem = hadoopPath.getFileSystem(hadoopConfiguration.value());
            FSDataInputStream inputStream = fileSystem.open(hadoopPath);
            return this.isGzip() ? new GZIPInputStream(inputStream) : inputStream;
        } catch (Exception e) {
            throw new ConnectorException(String.format(
                "Unable to read file at %s; cause: %s", filePath, e.getMessage()), e);
        }
    }

    public boolean isReadAbortOnFailure() {
        if (hasOption(Options.READ_FILES_ABORT_ON_FAILURE)) {
            return Boolean.parseBoolean(getStringOption(Options.READ_FILES_ABORT_ON_FAILURE));
        }
        return true;
    }

    byte[] readBytes(InputStream inputStream) throws IOException {
        byte[] bytes = FileUtil.readBytes(inputStream);
        return this.encoding != null ? new String(bytes, this.encoding).getBytes() : bytes;
    }

    public String getDecodedFilePath(FilePartition filePartition, int index) {
        String path = filePartition.getPaths().get(index);
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
}
