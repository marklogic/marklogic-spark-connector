/*
 * Copyright (c) 2023-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the zip bomb protection limits — READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES and
 * READ_ZIP_MAX_ENTRY_COUNT — are enforced across all zip-reading code paths when explicitly enabled.
 * Both limits default to -1 (disabled) and are opt-in.
 *
 * <p>Test zip files used:
 * <ul>
 *   <li>{@code zip-files/mixed-files.zip} — 4 entries (hello.json=23B, hello.txt=12B, hello.xml=21B, hello2.txt.gz=43B)
 *   <li>{@code aggregate-zips/employee-aggregates.zip} — 2 XML aggregate entries (412B and 222B uncompressed)
 *   <li>{@code rdf/each-rdf-file-type.zip} — 7 RDF entries (smallest: semantics.nq=759B)
 * </ul>
 */
class ReadZipBombProtectionTest extends AbstractIntegrationTest {

    // -----------------------------------------------------------------------
    // Generic zip (ZipFileReader path)
    // -----------------------------------------------------------------------

    @Test
    void genericZip_byteLimitThrowsConnectorException() {
        // hello.json is 23 bytes uncompressed; a limit of 10 must trigger an error.
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES, "10")
                .load("src/test/resources/zip-files/mixed-files.zip")
                .collectAsList()
        );
        assertTrue(ex.getMessage().contains("Zip entry uncompressed size exceeds"),
            "Expected byte-limit message, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES),
            "Error message should include the option name so the user knows how to fix it.");
    }

    @Test
    void genericZip_entryCountLimitThrowsConnectorException() {
        // mixed-files.zip has 4 entries; a limit of 2 must trigger an error on the 3rd entry.
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_ZIP_MAX_ENTRY_COUNT, "2")
                .load("src/test/resources/zip-files/mixed-files.zip")
                .collectAsList()
        );
        assertTrue(ex.getMessage().contains("Zip archive entry count exceeds"),
            "Expected entry-count message, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(Options.READ_ZIP_MAX_ENTRY_COUNT),
            "Error message should include the option name so the user knows how to fix it.");
    }

    @Test
    void genericZip_generousLimitAllowsNormalRead() {
        // A generous byte limit should not prevent normal operation.
        List<?> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .option(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES, "1048576") // 1 MB
            .option(Options.READ_ZIP_MAX_ENTRY_COUNT, "1000")
            .load("src/test/resources/zip-files/mixed-files.zip")
            .collectAsList();
        assertEquals(4, rows.size(), "All 4 entries should be read successfully within the generous limits.");
    }

    @Test
    void genericZip_defaultBehaviorAllowsUnlimitedRead() {
        // Both limits default to -1 (disabled), so all entries must be read without any configuration.
        List<?> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/zip-files/mixed-files.zip")
            .collectAsList();
        assertEquals(4, rows.size(), "Default behavior (limits disabled) should read all 4 entries.");
    }

    // -----------------------------------------------------------------------
    // Aggregate XML zip (ZipAggregateXmlFileReader path — internal bypass check)
    // -----------------------------------------------------------------------

    @Test
    void xmlZip_byteLimitThrowsConnectorException() {
        // employee-aggregates.zip first entry is 412 bytes; a limit of 100 must trigger an error.
        // This verifies the MaxBytesInputStream wrapping on the XML parser path (CWE-409 gap).
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_AGGREGATES_XML_ELEMENT, "employee")
                .option(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES, "100")
                .load("src/test/resources/aggregate-zips/employee-aggregates.zip")
                .collectAsList()
        );
        // The ConnectorException here may be the "Unable to process zip file" wrapper; the limit message
        // is in the cause. Accept either form, but only the byte-limit message — accepting the entry-count
        // message here would mask a regression where the byte-limit path is not actually exercised.
        String combinedMessage = ex.getMessage()
            + (ex.getCause() != null ? " " + ex.getCause().getMessage() : "");
        assertTrue(combinedMessage.contains("Zip entry uncompressed size exceeds"),
            "Expected byte-limit message, got: " + combinedMessage);
    }

    @Test
    void xmlZip_entryCountLimitThrowsConnectorException() {
        // employee-aggregates.zip has 2 entries; a limit of 1 must trigger an error on the 2nd.
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_AGGREGATES_XML_ELEMENT, "employee")
                .option(Options.READ_ZIP_MAX_ENTRY_COUNT, "1")
                .load("src/test/resources/aggregate-zips/employee-aggregates.zip")
                .collectAsList()
        );
        assertTrue(ex.getMessage().contains("Zip archive entry count exceeds"),
            "Expected entry-count message, got: " + ex.getMessage());
    }

    // -----------------------------------------------------------------------
    // RDF zip (RdfZipFileReader path — internal bypass check)
    // -----------------------------------------------------------------------

    @Test
    void rdfZip_byteLimitThrowsConnectorException() {
        // each-rdf-file-type.zip first entry (englishlocale.ttl) is 1309 bytes; limit of 500 must trigger.
        // This verifies the MaxBytesInputStream wrapping on the RDF parser path.
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_FILES_TYPE, "rdf")
                .option(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES, "500")
                .load("src/test/resources/rdf/each-rdf-file-type.zip")
                .collectAsList()
        );
        // RdfZipFileReader re-throws ConnectorException directly without wrapping it in a second
        // ConnectorException, so the limit message is on ex.getMessage() itself.
        assertTrue(ex.getMessage().contains("Zip entry uncompressed size exceeds"),
            "Expected byte-limit message, got: " + ex.getMessage());
    }

    @Test
    void rdfZip_entryCountLimitThrowsConnectorException() {
        // each-rdf-file-type.zip has 7 entries; a limit of 3 must trigger an error on the 4th.
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_FILES_TYPE, "rdf")
                .option(Options.READ_ZIP_MAX_ENTRY_COUNT, "3")
                .load("src/test/resources/rdf/each-rdf-file-type.zip")
                .collectAsList()
        );
        // RdfZipFileReader re-throws ConnectorException directly without wrapping it in a second
        // ConnectorException.
        assertTrue(ex.getMessage().contains("Zip archive entry count exceeds"),
            "Expected entry-count message, got: " + ex.getMessage());
    }

    // -----------------------------------------------------------------------
    // Input validation — invalid option values
    // -----------------------------------------------------------------------

    @Test
    void invalidByteLimitZero_throwsConnectorException() {
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES, "0")
                .load("src/test/resources/zip-files/mixed-files.zip")
                .collectAsList()
        );
        assertTrue(ex.getMessage().contains("Invalid value '0'"),
            "Expected invalid-value message for 0, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES),
            "Error message should include the option name.");
    }

    @Test
    void invalidByteLimitNegative_throwsConnectorException() {
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES, "-5")
                .load("src/test/resources/zip-files/mixed-files.zip")
                .collectAsList()
        );
        assertTrue(ex.getMessage().contains("Invalid value '-5'"),
            "Expected invalid-value message for -5, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(Options.READ_ZIP_MAX_UNCOMPRESSED_ENTRY_BYTES),
            "Error message should include the option name.");
    }

    @Test
    void invalidEntryCountZero_throwsConnectorException() {
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_ZIP_MAX_ENTRY_COUNT, "0")
                .load("src/test/resources/zip-files/mixed-files.zip")
                .collectAsList()
        );
        assertTrue(ex.getMessage().contains("Invalid value '0'"),
            "Expected invalid-value message for 0, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(Options.READ_ZIP_MAX_ENTRY_COUNT),
            "Error message should include the option name.");
    }

    @Test
    void invalidEntryCountNegative_throwsConnectorException() {
        ConnectorException ex = assertThrowsConnectorException(() ->
            newSparkSession().read()
                .format(CONNECTOR_IDENTIFIER)
                .option(Options.READ_FILES_COMPRESSION, "zip")
                .option(Options.READ_ZIP_MAX_ENTRY_COUNT, "-5")
                .load("src/test/resources/zip-files/mixed-files.zip")
                .collectAsList()
        );
        assertTrue(ex.getMessage().contains("Invalid value '-5'"),
            "Expected invalid-value message for -5, got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(Options.READ_ZIP_MAX_ENTRY_COUNT),
            "Error message should include the option name.");
    }
}
