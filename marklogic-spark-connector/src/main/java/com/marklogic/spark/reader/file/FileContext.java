/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
import com.marklogic.spark.Util;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.*;
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
            // Per the Spark BinaryFileFormat source code - calling getFileStatus seems to handle encoding in the file path.
            FileStatus fileStatus = fileSystem.getFileStatus(hadoopPath);
            FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath());
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

    /**
     * @return the maximum number of uncompressed bytes to read from a single zip entry;
     *         -1 means unlimited. Defaults to 256 MB.
     */
    public long getZipMaxUncompressedEntryBytes() {
        String val = getStringOption(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES);
        if (val != null && Long.parseLong(val) < 0) {
            Util.MAIN_LOGGER.warn("Zip bomb protection is disabled (option '{}' = {}). " +
                "Only set this on sources you fully trust.",
                Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES, val);
            return -1L;
        }
        return val != null ? Long.parseLong(val) : 268_435_456L;
    }

    /**
     * @return the maximum number of entries to iterate in a single zip archive;
     *         -1 means unlimited. Defaults to 100,000.
     */
    public int getZipMaxEntryCount() {
        String val = getStringOption(Options.READ_ZIP_MAX_ENTRY_COUNT);
        if (val != null && Integer.parseInt(val) < 0) {
            Util.MAIN_LOGGER.warn("Zip bomb protection is disabled (option '{}' = {}). " +
                "Only set this on sources you fully trust.",
                Options.READ_ZIP_MAX_ENTRY_COUNT, val);
            return -1;
        }
        return val != null ? Integer.parseInt(val) : 100_000;
    }

    /**
     * Wraps the given stream in a {@link MaxBytesInputStream} if the zip byte limit is configured,
     * otherwise returns the stream unchanged. Callers (such as XML and RDF parsers in sub-packages)
     * should use this instead of instantiating {@code MaxBytesInputStream} directly, since that
     * class is package-private to {@code com.marklogic.spark.reader.file}.
     *
     * @param in the raw zip entry stream
     * @return a byte-limited wrapper, or {@code in} if the limit is disabled (-1)
     */
    public InputStream boundedZipEntryStream(InputStream in) {
        long maxBytes = getZipMaxUncompressedEntryBytes();
        return maxBytes >= 0 ? new MaxBytesInputStream(in, maxBytes) : in;
    }

    byte[] readBytes(InputStream inputStream) throws IOException {
        byte[] bytes = FileUtil.readBytes(inputStream, getZipMaxUncompressedEntryBytes());
        return this.encoding != null ? new String(bytes, this.encoding).getBytes() : bytes;
    }

    private boolean isFileGzipped(String filePath, boolean guessIfGzipped) {
        if (this.isGzip()) {
            return true;
        }
        return guessIfGzipped && filePath != null && (filePath.endsWith(".gz") || filePath.endsWith(".gzip"));
    }
}
