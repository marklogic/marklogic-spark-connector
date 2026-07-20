/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.core;

import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Context;
import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pure unit tests for ServerEvaluationCallFactory path-validation logic.
 * No live MarkLogic connection is required.
 */
class ServerEvaluationCallFactoryTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static ServerEvaluationCallFactory.Builder newJsFileBuilder() {
        return new ServerEvaluationCallFactory.Builder()
            .withJavascriptFileOptionName(Options.READ_JAVASCRIPT_FILE);
    }

    private static ServerEvaluationCallFactory.Builder newXqyFileBuilder() {
        return new ServerEvaluationCallFactory.Builder()
            .withXqueryFileOptionName(Options.READ_XQUERY_FILE);
    }

    private static ServerEvaluationCallFactory.Builder newWriteJsFileBuilder() {
        return new ServerEvaluationCallFactory.Builder()
            .withJavascriptFileOptionName(Options.WRITE_JAVASCRIPT_FILE);
    }

    private static ServerEvaluationCallFactory.Builder newWriteXqyFileBuilder() {
        return new ServerEvaluationCallFactory.Builder()
            .withXqueryFileOptionName(Options.WRITE_XQUERY_FILE);
    }

    private static Context contextWith(String key, String value) {
        Map<String, String> props = new HashMap<>();
        props.put(key, value);
        return new Context(props);
    }

    private static Context contextWith(String k1, String v1, String k2, String v2) {
        Map<String, String> props = new HashMap<>();
        props.put(k1, v1);
        props.put(k2, v2);
        return new Context(props);
    }

    // -----------------------------------------------------------------------
    // Happy-path: relative path within CWD
    // -----------------------------------------------------------------------

    @Test
    void relativePathWithinCwd_buildSucceeds() {
        // Uses a file that exists in the test resources directory, resolved relative to CWD
        // (the Gradle test task runs with CWD = marklogic-spark-connector/marklogic-spark-connector)
        Context context = contextWith(Options.READ_JAVASCRIPT_FILE,
            "src/test/resources/custom-code/my-reader.js");

        Optional<ServerEvaluationCallFactory> factory = newJsFileBuilder().build(context);

        assertTrue(factory.isPresent(), "Build should succeed for a CWD-relative path");
    }

    @Test
    void absolutePathWithinCwd_buildSucceeds() {
        String cwd = System.getProperty("user.dir");
        String absPath = cwd + "/src/test/resources/custom-code/my-reader.js";

        Context context = contextWith(Options.READ_JAVASCRIPT_FILE, absPath);

        Optional<ServerEvaluationCallFactory> factory = newJsFileBuilder().build(context);

        assertTrue(factory.isPresent(), "Build should succeed for an absolute path inside CWD");
    }

    // -----------------------------------------------------------------------
    // Path traversal — resolves outside CWD
    // -----------------------------------------------------------------------

    @Test
    void dotDotTraversalOutsideCwd_throwsConnectorException() {
        Context context = contextWith(Options.READ_JAVASCRIPT_FILE, "../../secret.js");

        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> newJsFileBuilder().build(context));

        assertTrue(ex.getMessage().contains("is not within the permitted directory"),
            "Expected traversal error message, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(Options.SCRIPT_FILE_ALLOWED_PATHS),
            "Error message should mention the SCRIPT_FILE_ALLOWED_PATHS option");
    }

    @Test
    void xqueryFile_dotDotTraversalOutsideCwd_throwsConnectorException() {
        Context context = contextWith(Options.READ_XQUERY_FILE, "../../secret.xqy");

        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> newXqyFileBuilder().build(context));

        assertTrue(ex.getMessage().contains("is not within the permitted directory"),
            "Expected traversal error message, got: " + ex.getMessage());
    }

    @Test
    void writeJavascriptFile_dotDotTraversalOutsideCwd_throwsConnectorException() {
        Context context = contextWith(Options.WRITE_JAVASCRIPT_FILE, "../../secret.js");

        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> newWriteJsFileBuilder().build(context));

        assertTrue(ex.getMessage().contains("is not within the permitted directory"),
            "Expected traversal error message for WRITE_JAVASCRIPT_FILE, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(Options.SCRIPT_FILE_ALLOWED_PATHS),
            "Error message should mention the SCRIPT_FILE_ALLOWED_PATHS option");
    }

    @Test
    void writeXqueryFile_dotDotTraversalOutsideCwd_throwsConnectorException() {
        Context context = contextWith(Options.WRITE_XQUERY_FILE, "../../secret.xqy");

        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> newWriteXqyFileBuilder().build(context));

        assertTrue(ex.getMessage().contains("is not within the permitted directory"),
            "Expected traversal error message for WRITE_XQUERY_FILE, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(Options.SCRIPT_FILE_ALLOWED_PATHS),
            "Error message should mention the SCRIPT_FILE_ALLOWED_PATHS option");
    }

    // -----------------------------------------------------------------------
    // Absolute path outside CWD (sensitive-file vector)
    // -----------------------------------------------------------------------

    @Test
    void absolutePathOutsideCwd_throwsConnectorException(@TempDir Path tempDir) throws IOException {
        // Write a file in a temp dir that is guaranteed to be outside CWD
        Path sensitiveFile = tempDir.resolve("credentials.txt");
        Files.writeString(sensitiveFile, "secret");

        Context context = contextWith(Options.READ_JAVASCRIPT_FILE, sensitiveFile.toString());

        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> newJsFileBuilder().build(context));

        assertTrue(ex.getMessage().contains("is not within the permitted directory"),
            "Expected path-restriction error, got: " + ex.getMessage());
    }

    // -----------------------------------------------------------------------
    // SCRIPT_FILE_ALLOWED_PATHS — explicit allow-list
    // -----------------------------------------------------------------------

    @Test
    void allowedPathsOption_permitsFileInAllowedDirectory(@TempDir Path tempDir) throws IOException {
        Path scriptFile = tempDir.resolve("my-script.js");
        Files.writeString(scriptFile, "Sequence.from(['ok'])");

        Context context = contextWith(
            Options.READ_JAVASCRIPT_FILE, scriptFile.toString(),
            Options.SCRIPT_FILE_ALLOWED_PATHS, tempDir.toAbsolutePath().toString());

        Optional<ServerEvaluationCallFactory> factory = newJsFileBuilder().build(context);

        assertTrue(factory.isPresent(), "Build should succeed when file is within an explicitly allowed directory");
    }

    @Test
    void allowedPathsOption_multipleEntries_permitsFileInSecondEntry(@TempDir Path tempDir) throws IOException {
        Path otherDir = tempDir.resolve("scripts");
        Files.createDirectories(otherDir);
        Path scriptFile = otherDir.resolve("reader.js");
        Files.writeString(scriptFile, "Sequence.from(['ok'])");

        // First entry is a different (non-matching) directory; second entry matches
        String allowedPaths = tempDir.resolve("nonexistent").toAbsolutePath() + ";" + otherDir.toAbsolutePath();
        Context context = contextWith(
            Options.READ_JAVASCRIPT_FILE, scriptFile.toString(),
            Options.SCRIPT_FILE_ALLOWED_PATHS, allowedPaths);

        Optional<ServerEvaluationCallFactory> factory = newJsFileBuilder().build(context);

        assertTrue(factory.isPresent(), "Build should succeed when path matches second entry in allowed list");
    }

    @Test
    void allowedPathsOption_set_butPathOutsideIt_throwsConnectorException(@TempDir Path tempDir) throws IOException {
        Path allowedDir = tempDir.resolve("allowed");
        Files.createDirectories(allowedDir);

        Path otherDir = tempDir.resolve("other");
        Files.createDirectories(otherDir);
        Path scriptFile = otherDir.resolve("sneaky.js");
        Files.writeString(scriptFile, "xdmp.log('exfiltrated')");

        Context context = contextWith(
            Options.READ_JAVASCRIPT_FILE, scriptFile.toString(),
            Options.SCRIPT_FILE_ALLOWED_PATHS, allowedDir.toAbsolutePath().toString());

        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> newJsFileBuilder().build(context));

        assertTrue(ex.getMessage().contains("is not within the permitted directory"),
            "Expected path-restriction error, got: " + ex.getMessage());
    }

    // -----------------------------------------------------------------------
    // Null-byte boundary test
    // -----------------------------------------------------------------------

    @Test
    void pathWithNullByte_throwsException() {
        // A null byte in the path should fail either at the ConnectorException
        // validation stage or as an IOException from the filesystem.
        Context context = contextWith(Options.READ_JAVASCRIPT_FILE,
            "src/test/resources/custom-code/my-reader.js\u0000");

        assertThrows(Exception.class, () -> newJsFileBuilder().build(context),
            "A path with an embedded null byte should throw an exception");
    }

    // -----------------------------------------------------------------------
    // Non-existent file within allowed paths — existing error message preserved
    // -----------------------------------------------------------------------

    @Test
    void nonExistentFileWithinCwd_preservesWasNotFoundMessage() {
        Context context = contextWith(Options.READ_JAVASCRIPT_FILE,
            "src/test/resources/custom-code/does-not-exist.js");

        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> newJsFileBuilder().build(context));

        assertTrue(ex.getMessage().contains("was not found."),
            "Expected 'was not found.' in error message, got: " + ex.getMessage());
    }

    @Test
    void nonExistentFileExplicitlyAllowed_preservesWasNotFoundMessage(@TempDir Path tempDir) {
        // File is in the allowed directory but does not actually exist on disk
        Path nonExistentFile = tempDir.resolve("ghost.js");

        Context context = contextWith(
            Options.READ_JAVASCRIPT_FILE, nonExistentFile.toString(),
            Options.SCRIPT_FILE_ALLOWED_PATHS, tempDir.toAbsolutePath().toString());

        ConnectorException ex = assertThrows(ConnectorException.class,
            () -> newJsFileBuilder().build(context));

        assertTrue(ex.getMessage().contains("was not found."),
            "Expected 'was not found.' in error message, got: " + ex.getMessage());
    }
}
