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

class ReadAggregateXMLZipFilesTest extends AbstractIntegrationTest {

    @Test
    void zipWithTwoAggregateXMLFiles() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/aggregate-zips/employee-aggregates.zip")
            .collectAsList();

        assertEquals(4, rows.size(), "The first XML file in the zip has 3 employees and the second has 1.");
        String rootPath = "/Employee/";
        // Expected pattern is "file-entry number-aggregate number.xml".
        verifyRow(rows.get(0), "employee-aggregates.zip-1-1.xml", rootPath, "John", 40);
        verifyRow(rows.get(1), "employee-aggregates.zip-1-2.xml", rootPath, "Jane", 41);
        verifyRow(rows.get(2), "employee-aggregates.zip-1-3.xml", rootPath, "Brenda", 42);
        verifyRow(rows.get(3), "employee-aggregates.zip-2-1.xml", rootPath, "Linda", 43);
    }

    @Test
    void uriElementHasMixedContent() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "mixed")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/aggregate-zips/employee-aggregates.zip");

        ConnectorException ex = assertThrowsConnectorException(() -> dataset.count());
        String message = ex.getMessage();
        assertTrue(
            message.startsWith(
                "Unable to get text from URI element 'mixed' found in aggregate element 1 in entry employees.xml in file:///"
            ),
            "The error should identify the URI element that text could not be retrieved from along with which aggregate " +
                "element and which zip entry produced the failure; actual message: " + message
        );
    }

    @Test
    void uriElementNotFound() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "elementDoesntExist")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/aggregate-zips/employee-aggregates.zip");

        ConnectorException ex = assertThrowsConnectorException(() -> dataset.count());
        String message = ex.getMessage();
        assertTrue(
            message.startsWith("No occurrence of URI element 'elementDoesntExist' found in aggregate element 1 in entry employees.xml in file:///"),
            "The error should identify which aggregate element did not contain the URI element; actual error: " + message
        );
    }

    @Test
    void notZipFile() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "name")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/500-employees.json");

        assertEquals(0, dataset.count(), "Java's ZipInputStream does not throw an error when applied to a file " +
            "that is not a zip. Instead, it does not returning any occurrences of ZipEntry. So no error occurs, " +
            "which is the same behavior as in MLCP.");
    }

    @Test
    void notXmlFileInZip() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "name")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load("src/test/resources/aggregate-zips/json-employees.zip");

        ConnectorException ex = assertThrowsConnectorException(() -> dataset.count());
        String message = ex.getMessage();
        assertTrue(message.startsWith("Unable to read XML from entry 500-employees.json in file:///"),
            "The error should identify the file and the entry name; actual error: " + message);
        assertTrue(message.endsWith("json-employees.zip; cause: Failed to traverse document"),
            "The error should identify the file and the root cause; actual error: " + message);
    }

    private void verifyRow(Row row, String expectedUriSuffix, String rootPath, String name, int age) {
        String uri = row.getString(0);
        assertTrue(uri.endsWith(expectedUriSuffix), String.format("URI %s doesn't end with %s", uri, expectedUriSuffix));
        String xml = new String((byte[]) row.get(3));
        XmlNode doc = new XmlNode(xml, Namespace.getNamespace("ex", "org:example"));
        doc.assertElementValue(String.format("%sname", rootPath), name);
        doc.assertElementValue(String.format("%sage", rootPath), Integer.toString(age));
    }
}
