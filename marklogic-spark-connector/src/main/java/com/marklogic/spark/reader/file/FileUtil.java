/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public interface FileUtil {

    /**
     * Does not handle file encoding - {@code FileContext} is expected to handle that as it has access to the
     * user's options.
     *
     * @param inputStream
     * @return
     * @throws IOException
     */
    static byte[] readBytes(InputStream inputStream) throws IOException {
        return readBytes(inputStream, -1L);
    }

    /**
     * Reads all bytes from {@code inputStream} into a byte array, enforcing an optional upper bound on the
     * number of bytes read. Pass {@code -1} for {@code maxBytes} to disable the limit.
     *
     * <p>Uses a 4096-byte read buffer (an improvement over the legacy 1024-byte buffer used prior to 3.1.2),
     * which benefits all callers including non-zip readers.
     *
     * @param inputStream the stream to drain
     * @param maxBytes    the maximum number of bytes to allow; -1 means unlimited
     * @return the bytes read
     * @throws IOException          on I/O errors
     * @throws ConnectorException   when {@code maxBytes} is non-negative and the limit is exceeded
     */
    static byte[] readBytes(InputStream inputStream, long maxBytes) throws IOException {
        byte[] buffer = new byte[4096];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long totalRead = 0;
        int read;
        while ((read = inputStream.read(buffer)) != -1) {
            totalRead += read;
            if (maxBytes >= 0 && totalRead > maxBytes) {
                throw new ConnectorException(String.format(
                    "Zip entry uncompressed size exceeds the maximum of %d bytes. " +
                        "Use connector option '%s' to increase or disable this limit (set to -1 to disable).",
                    maxBytes, Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES));
            }
            baos.write(buffer, 0, read);
        }
        return baos.toByteArray();
    }

    // ZipInputStream does not expose entry size metadata (getSize() / getCompressedSize() return -1 before content
    // is read). Compensating controls are applied by all callers:
    //   - Per-entry byte limit: enforced in FileContext.readBytes() for the direct-read path, and via
    //     MaxBytesInputStream for the XML and RDF parser paths (READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES).
    //   - Per-archive entry count: enforced by each reader before calling this method (READ_ZIP_MAX_ENTRY_COUNT).
    static ZipEntry findNextFileEntry(ZipInputStream zipInputStream) throws IOException {
        ZipEntry entry = zipInputStream.getNextEntry();
        if (entry == null) {
            return null;
        }
        return !entry.isDirectory() ? entry : findNextFileEntry(zipInputStream);
    }

    static FilePartition[] makeFilePartitions(String[] files, int numPartitions) {
        // Files can be empty when, for example, a glob pattern doesn't match any files.
        if (files == null || files.length == 0) {
            return new FilePartition[]{};
        }

        if (numPartitions <= 0) {
            // Divide-by-zero protection.
            numPartitions = 1;
        }

        if (files.length < numPartitions) {
            numPartitions = files.length;
        }

        final FilePartition[] partitions = new FilePartition[numPartitions];

        // Distribute files across partitions as evenly as possible, ensuring that no partition is empty.
        final int baseFilesPerPartition = files.length / numPartitions;
        final int remainingFiles = files.length % numPartitions;

        int fileIndex = 0;
        for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
            // First 'remainingFiles' partitions get one extra file
            int filesForThisPartition = baseFilesPerPartition + (partitionIndex < remainingFiles ? 1 : 0);

            List<String> currentPartition = new ArrayList<>();
            for (int i = 0; i < filesForThisPartition; i++) {
                currentPartition.add(files[fileIndex++]);
            }
            partitions[partitionIndex] = new FilePartition(currentPartition);
        }

        return partitions;
    }

    static byte[] serializeFileContext(FileContext fileContext, String currentFilePath) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(fileContext);
            oos.flush();
            return baos.toByteArray();
        } catch (Exception ex) {
            String message = String.format("Unable to build row for file at %s; cause: %s",
                currentFilePath, ex.getMessage());
            throw new ConnectorException(message, ex);
        }
    }
}
