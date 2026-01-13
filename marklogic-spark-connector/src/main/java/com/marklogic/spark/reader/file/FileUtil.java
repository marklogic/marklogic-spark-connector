/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.ConnectorException;

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
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int read;
        while ((read = inputStream.read(buffer)) != -1) {
            baos.write(buffer, 0, read);
        }
        return baos.toByteArray();
    }

    // The suppressed Sonar warning is for a potential "Zip bomb" attack when accessing a zip entry. The recommended
    // fixes involve checking the zip of the entry. That is not possible as the ZipInputStream returns -1 for both the
    // size and compressed size.
    @SuppressWarnings("java:S5042")
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
