/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import com.marklogic.spark.reader.document.DocumentRowSchema;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that a failure to finish writing a file is propagated instead of being silently ignored.
 */
class WriteFilesCloseFailureTest {

    private SparkSession session;

    @AfterEach
    void afterEach() {
        FailingFileSystem.failOnClose = false;
        UriCountTestListener.reset();
        if (session != null) {
            session.stop();
        }
    }

    @Test
    void zipCommitFailsWhenCloseFails(@TempDir Path tempDir) throws Exception {
        FailingFileSystem.failOnClose = true;
        Map<String, String> properties = newProperties(tempDir);
        properties.put(Options.WRITE_FILES_LISTENER_CLASS_NAME, UriCountTestListener.class.getName());

        ZipFileWriter writer = new ZipFileWriter(properties, newHadoopConfig(), 0);
        writer.write(makeRow("/doc1.json"));

        ConnectorException ex = assertThrows(ConnectorException.class, writer::commit);
        assertTrue(ex.getMessage().startsWith("Unable to finish writing zip file " + writer.getZipFilePath()),
            "The error should identify the zip file so the user can decide what to do with it; error: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(FailingFileSystem.FAILURE_MESSAGE), "Unexpected error: " + ex.getMessage());
        assertTrue(UriCountTestListener.writtenUriCounts.isEmpty(), "The final listener callback should not be " +
            "invoked when the zip file could not be finished.");

        assertDoesNotThrow(writer::close, "Spark calls close after a failed commit; the zip file should not be " +
            "closed a second time.");
        assertEquals(1, tempDir.toFile().listFiles().length, "The connector should not delete the file; the user " +
            "decides what to do with it.");
    }

    @Test
    void zipCloseFailsWithoutCommit(@TempDir Path tempDir) throws Exception {
        FailingFileSystem.failOnClose = true;

        ZipFileWriter writer = new ZipFileWriter(newProperties(tempDir), newHadoopConfig(), 0);
        writer.write(makeRow("/doc1.json"));

        ConnectorException ex = assertThrows(ConnectorException.class, writer::close);
        assertTrue(ex.getMessage().startsWith("Unable to finish writing zip file " + writer.getZipFilePath()),
            "Unexpected error: " + ex.getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void documentFileCloseFailureIsPropagated(boolean gzip, @TempDir Path tempDir) {
        FailingFileSystem.failOnClose = true;
        Map<String, String> properties = newProperties(tempDir);
        SerializableConfiguration config = newHadoopConfig();
        DocumentFileWriter writer = gzip ? new GzipFileWriter(properties, config) : new DocumentFileWriter(properties, config);

        IOException ex = assertThrows(IOException.class, () -> writer.write(makeRow("/doc1.json")));
        assertEquals(FailingFileSystem.FAILURE_MESSAGE, ex.getMessage());
        writer.close();
    }

    @Test
    void sparkJobFailsWhenZipCannotBeFinished(@TempDir Path tempDir) {
        FailingFileSystem.failOnClose = true;
        session = SparkSession.builder().master("local[2]").getOrCreate();
        FailingFileSystem.register(session.sparkContext().hadoopConfiguration());

        List<Row> rows = List.of(
            RowFactory.create("/doc1.json", "{}".getBytes(), "JSON", null, null, null, null, null),
            RowFactory.create("/doc2.json", "{}".getBytes(), "JSON", null, null, null, null, null)
        );

        Exception ex = assertThrows(Exception.class, () -> session.createDataFrame(rows, DocumentRowSchema.SCHEMA)
            .repartition(1)
            .write()
            .format("marklogic")
            .option(Options.WRITE_FILES_COMPRESSION, "zip")
            .mode(SaveMode.Append)
            .save(FailingFileSystem.toPath(tempDir)));

        String stackTrace = ExceptionUtils.getStackTrace(ex);
        assertTrue(stackTrace.contains("Unable to finish writing zip file"), stackTrace);
        assertTrue(stackTrace.contains(FailingFileSystem.FAILURE_MESSAGE), stackTrace);
    }

    private Map<String, String> newProperties(Path tempDir) {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", FailingFileSystem.toPath(tempDir));
        return properties;
    }

    private SerializableConfiguration newHadoopConfig() {
        Configuration config = new Configuration();
        FailingFileSystem.register(config);
        return new SerializableConfiguration(config);
    }

    private GenericInternalRow makeRow(String uri) {
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(uri),
            "{}".getBytes(),
            UTF8String.fromString("JSON"),
            null, null, null, null, null
        });
    }
}
