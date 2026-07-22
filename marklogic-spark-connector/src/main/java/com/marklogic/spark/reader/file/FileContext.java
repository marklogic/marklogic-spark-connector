/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.ContextSupport;
import com.marklogic.spark.Options;
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
     *         -1 means unlimited (the default). Zip bomb protection is opt-in: set a positive
     *         integer to enable the limit.
     * @throws ConnectorException if the configured value is 0 or a negative number other than -1,
     *         since those values have no meaningful interpretation.
     */
    public long getZipMaxUncompressedEntryBytes() {
        String val = getStringOption(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES);
        long limit = val != null ? Long.parseLong(val) : -1L;
        if (limit == 0 || (limit < 0 && limit != -1L)) {
            throw new ConnectorException(String.format(
                "Invalid value '%s' for option '%s': must be -1 (disabled) or a positive integer.",
                val, Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES));
        }
        return limit;
    }

    /**
     * @return the maximum number of entries to iterate in a single zip archive;
     *         -1 means unlimited (the default). Zip bomb protection is opt-in: set a positive
     *         integer to enable the limit.
     * @throws ConnectorException if the configured value is 0 or a negative number other than -1,
     *         since those values have no meaningful interpretation.
     */
    public int getZipMaxEntryCount() {
        String val = getStringOption(Options.READ_ZIP_MAX_ENTRY_COUNT);
        int limit = val != null ? Integer.parseInt(val) : -1;
        if (limit == 0 || (limit < 0 && limit != -1)) {
            throw new ConnectorException(String.format(
                "Invalid value '%s' for option '%s': must be -1 (disabled) or a positive integer.",
                val, Options.READ_ZIP_MAX_ENTRY_COUNT));
        }
        return limit;
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
        // Only apply the zip entry byte limit when reading from a zip-based format
        // (compression=zip, type=archive, or type=mlcp_archive). Non-zip callers such as
        // GenericFileReader and GzipFileReader must not be subject to the zip-specific limit.
        long maxBytes = isZipBasedRead() ? getZipMaxUncompressedEntryBytes() : -1L;
        byte[] bytes = FileUtil.readBytes(inputStream, maxBytes);
        return this.encoding != null ? new String(bytes, this.encoding).getBytes() : bytes;
    }

    /**
     * @return true when the current read context is a zip-based format — i.e. compression=zip,
     *         type=archive, or type=mlcp_archive. Used to scope the zip byte limit to zip readers only.
     */
    private boolean isZipBasedRead() {
        if (isZip()) {
            return true;
        }
        String fileType = getStringOption(Options.READ_FILES_TYPE);
        return "archive".equalsIgnoreCase(fileType) || "mlcp_archive".equalsIgnoreCase(fileType);
    }

    private boolean isFileGzipped(String filePath, boolean guessIfGzipped) {
        if (this.isGzip()) {
            return true;
        }
        return guessIfGzipped && filePath != null && (filePath.endsWith(".gz") || filePath.endsWith(".gzip"));
    }
}
