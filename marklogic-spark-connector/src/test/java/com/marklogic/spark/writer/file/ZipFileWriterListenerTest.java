/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.Options;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that ZipFileWriter reports progress via a configured FileWriteListener based on the number of URIs
 * processed, not the number of zip entries written (a document with metadata results in two zip entries but should
 * only count as one URI).
 */
class ZipFileWriterListenerTest {

    @AfterEach
    void reset() {
        UriCountTestListener.reset();
    }

    @Test
    void noListenerConfigured(@TempDir Path tempDir) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", tempDir.toFile().getAbsolutePath());

        ZipFileWriter writer = new ZipFileWriter(properties, new SerializableConfiguration(newHadoopConfig()), 0);
        writer.write(makeContentOnlyRow("/doc1.json"));
        writer.commit();
        writer.close();

        assertTrue(UriCountTestListener.writtenUriCounts.isEmpty(), "No callbacks should fire when no listener class " +
            "is configured.");
    }

    @Test
    void oneUriPerRowRegardlessOfMetadataEntry(@TempDir Path tempDir) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", tempDir.toFile().getAbsolutePath());
        properties.put(Options.WRITE_FILES_LISTENER_CLASS_NAME, UriCountTestListener.class.getName());
        properties.put(Options.WRITE_FILES_LISTENER_PROGRESS_INTERVAL, "1");
        properties.put(Options.WRITE_FILES_LISTENER_PARAM_PREFIX + "exportId", "export-123");

        ZipFileWriter writer = new ZipFileWriter(properties, new SerializableConfiguration(newHadoopConfig()), 0);
        writer.write(makeContentOnlyRow("/doc1.json"));
        writer.write(makeRowWithMetadata("/doc2.json"));
        writer.commit();
        writer.close();

        assertEquals("export-123", UriCountTestListener.params.get("exportId"));
        // With an interval of 1, each write() call crosses the threshold (1, then 2); commit() also always fires a
        // final callback with the true final count (2 again here, since it already lands on a boundary).
        assertEquals(java.util.List.of(1L, 2L, 2L), UriCountTestListener.writtenUriCounts, "Each write() call should " +
            "be counted as exactly one URI, even though the second row also produces a metadata zip entry (two zip " +
            "entries total for that row). Logged counts: " + UriCountTestListener.writtenUriCounts);

        ZipFile zipFile = new ZipFile(writer.getZipFilePath());
        assertNotNull(zipFile.getEntry("/doc1.json"));
        assertNotNull(zipFile.getEntry("/doc2.json"));
        zipFile.close();
    }

    @Test
    void finalCallbackFiresOnCommitEvenWithoutCrossingInterval(@TempDir Path tempDir) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", tempDir.toFile().getAbsolutePath());
        properties.put(Options.WRITE_FILES_LISTENER_CLASS_NAME, UriCountTestListener.class.getName());
        // A large interval that will never be crossed by the 2 rows written below.
        properties.put(Options.WRITE_FILES_LISTENER_PROGRESS_INTERVAL, "1000");

        ZipFileWriter writer = new ZipFileWriter(properties, new SerializableConfiguration(newHadoopConfig()), 0);
        writer.write(makeContentOnlyRow("/doc1.json"));
        writer.write(makeContentOnlyRow("/doc2.json"));
        writer.commit();
        writer.close();

        assertEquals(java.util.List.of(2L), UriCountTestListener.writtenUriCounts, "Even though the progress " +
            "interval of 1000 was never crossed, commit() should still report the true final count so the last " +
            "partial interval isn't lost. Logged counts: " + UriCountTestListener.writtenUriCounts);
    }

    @Test
    void noProgressIntervalConfiguredOnlyFiresFinalCallback(@TempDir Path tempDir) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", tempDir.toFile().getAbsolutePath());
        properties.put(Options.WRITE_FILES_LISTENER_CLASS_NAME, UriCountTestListener.class.getName());
        // Not setting WRITE_FILES_LISTENER_PROGRESS_INTERVAL at all - defaults to 0, which disables periodic callbacks.

        ZipFileWriter writer = new ZipFileWriter(properties, new SerializableConfiguration(newHadoopConfig()), 0);
        writer.write(makeContentOnlyRow("/doc1.json"));
        writer.write(makeContentOnlyRow("/doc2.json"));
        writer.write(makeContentOnlyRow("/doc3.json"));
        writer.commit();
        writer.close();

        assertEquals(java.util.List.of(3L), UriCountTestListener.writtenUriCounts, "With no progress interval " +
            "configured, only the final commit() callback should fire.");
    }

    private org.apache.hadoop.conf.Configuration newHadoopConfig() {
        return new org.apache.hadoop.conf.Configuration();
    }

    private GenericInternalRow makeContentOnlyRow(String uri) {
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri),
            "{}".getBytes(),
            UTF8String.fromString("JSON"),
            null, null, null, null, null
        });
    }

    private GenericInternalRow makeRowWithMetadata(String uri) {
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri),
            "{}".getBytes(),
            UTF8String.fromString("JSON"),
            org.apache.spark.sql.catalyst.util.ArrayData.toArrayData(new UTF8String[]{UTF8String.fromString("test-collection")}),
            null, null, null, null
        });
    }
}
