/*
 * Copyright (c) 2023-2025 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.spark.reader.file;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jdom2.Namespace;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadGzipAggregateXmlFilesTest extends AbstractIntegrationTest {

    @Test
    void noNamespace() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load("src/test/resources/aggregate-gzips/employees.xml.gz")
            .collectAsList();

        assertEquals(3, rows.size());
        String rootPath = "/Employee/";
        verifyRow(rows.get(0), "employees.xml.gz-1.xml", rootPath, "John", 40);
        verifyRow(rows.get(1), "employees.xml.gz-2.xml", rootPath, "Jane", 41);
        verifyRow(rows.get(2), "employees.xml.gz-3.xml", rootPath, "Brenda", 42);
    }

    @Test
    void nonGZIPFile() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .load("src/test/resources/aggregates/employees.xml");

        ConnectorException ex = assertThrowsConnectorException(dataset::count);
        String message = ex.getMessage();
        assertTrue(message.startsWith("Unable to read file at file:/"), "Unexpected error: " + message);
        assertTrue(message.endsWith("cause: Not in GZIP format"), "Unexpected error: " + message);
    }

    @Test
    void ignoreInvalidGzipFile() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_FILES_COMPRESSION, "gzip")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load("src/test/resources/aggregates/employees.xml", "src/test/resources/aggregate-gzips/employees.xml.gz")
            .collectAsList();

        assertEquals(3, rows.size(), "The error from the non-gzipped file should be ignored and logged, and the " +
            "3 rows from the valid gzip file should be returned.");
    }

    private void verifyRow(Row row, String expectedUriSuffix, String rootPath, String name, int age) {
        String uri = row.getString(0);
        assertTrue(uri.endsWith(expectedUriSuffix), String.format("URI %s doesn't end with %s", uri, expectedUriSuffix));
        String xml = new String((byte[]) row.get(1));
        XmlNode doc = new XmlNode(xml, Namespace.getNamespace("ex", "org:example"));
        doc.assertElementValue(String.format("%sname", rootPath), name);
        doc.assertElementValue(String.format("%sage", rootPath), Integer.toString(age));
        assertEquals("xml", row.getString(2));
    }
}
