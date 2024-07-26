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

class ReadAggregateXmlZipFilesTest extends AbstractIntegrationTest {

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
    void twoZipsOnePartition() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .load(
                "src/test/resources/aggregate-zips/employee-aggregates.zip",
                "src/test/resources/aggregate-zips/employee-aggregates-copy.zip"
            )
            .collectAsList();

        assertEquals(8, rows.size(), "The two zip files, each containing 4 rows, should be read by a single " +
            "partition reader that iterates over the two file paths.");
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

    @Test
    void ignoreUriElementNotFound() {
        long count = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "id")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load("src/test/resources/aggregate-zips/employee-aggregates.zip")
            .count();

        assertEquals(3, count, "The element without an 'id' element should be ignored and an error should be logged " +
            "for it, and then the 3 elements with an 'id' child element should be returned as rows. 2 of those " +
            "elements come from employees.xml, and the 3rd comes from employees2.xml.");
    }

    @Test
    void ignoreBadFileInZip() {
        long count = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load("src/test/resources/aggregate-zips/xml-and-json.zip")
            .count();

        assertEquals(3, count, "The JSON file in the zip should result in the error being logged, and the valid " +
            "XML file should still be processed.");
    }

    @Test
    void ignoreAllBadFilesInZip() {
        long count = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_FILES_COMPRESSION, "zip")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load("src/test/resources/zip-files/mixed-files.zip")
            .count();

        assertEquals(0, count, "Every file in mixed-files.zip is either not XML or it's an XML document " +
            "with no occurrences of 'Employee', so 0 rows should be returned.");
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
