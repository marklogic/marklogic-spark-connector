package com.marklogic.spark.reader.file;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
        int filesPerPartition = (int) Math.ceil((double) files.length / (double) numPartitions);
        if (files.length < numPartitions) {
            numPartitions = files.length;
        }
        final FilePartition[] partitions = new FilePartition[numPartitions];
        List<String> currentPartition = new ArrayList<>();
        int partitionIndex = 0;
        for (int i = 0; i < files.length; i++) {
            if (currentPartition.size() == filesPerPartition) {
                partitions[partitionIndex] = new FilePartition(currentPartition);
                partitionIndex++;
                currentPartition = new ArrayList<>();
            }
            currentPartition.add(files[i]);
        }
        if (!currentPartition.isEmpty()) {
            partitions[partitionIndex] = new FilePartition(currentPartition);
        }
        return partitions;
    }
}
