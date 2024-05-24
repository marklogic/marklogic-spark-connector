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

class ReadAggregateXmlFilesTest extends AbstractIntegrationTest {

    @Test
    void noNamespace() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_NUM_PARTITIONS, 1)
            .load("src/test/resources/aggregates")
            .collectAsList();

        assertEquals(3, rows.size());
        String rootPath = "/Employee/";
        verifyRow(rows.get(0), "employees.xml-1.xml", rootPath, "John", 40);
        verifyRow(rows.get(1), "employees.xml-2.xml", rootPath, "Jane", 41);
        verifyRow(rows.get(2), "employees.xml-3.xml", rootPath, "Brenda", 42);
    }

    @Test
    void withNamespace() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_NAMESPACE, "org:example")
            .load("src/test/resources/aggregates")
            .collectAsList();

        assertEquals(3, rows.size());
        String rootPath = "/ex:Employee/ex:";
        verifyRow(rows.get(0), "employees-namespace.xml-1.xml", rootPath, "John", 40);
        verifyRow(rows.get(1), "employees-namespace.xml-2.xml", rootPath, "Jane", 41);
        verifyRow(rows.get(2), "employees-namespace.xml-3.xml", rootPath, "Brenda", 42);
    }

    @Test
    void noMatchingElements() {
        long count = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "ElementThatDoesntExist")
            .load("src/test/resources/aggregates")
            .count();

        assertEquals(0, count);
    }

    @Test
    void withUriElement() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "name")
            .load("src/test/resources/aggregates")
            .collectAsList();

        // Verify the URI (column 0) is the value of the 'name' element for each row.
        assertEquals("John", rows.get(0).getString(0));
        assertEquals("Jane", rows.get(1).getString(0));
        assertEquals("Brenda", rows.get(2).getString(0));
    }

    /**
     * This behaves slightly different from MLCP, which only allows for a local name to be specified for "uri_id".
     * That can be limiting though - e.g. if an XML fragment has both 'ns1:value' and 'ns2:value', it's not possible
     * to specify either one. So the connector requires that the namespace be specified for the URI element as well,
     * if a namespace exists.
     */
    @Test
    void uriElementHasNamespace() {
        List<Row> rows = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_NAMESPACE, "org:example")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "name")
            .option(Options.READ_AGGREGATES_XML_URI_NAMESPACE, "org:example")
            .load("src/test/resources/aggregates/employees-namespace.xml")
            .collectAsList();

        // Verify the URI (column 0) is the value of the 'name' element for each row.
        assertEquals("John", rows.get(0).getString(0));
        assertEquals("Jane", rows.get(1).getString(0));
        assertEquals("Brenda", rows.get(2).getString(0));
    }

    @Test
    void uriElementHasMixedContent() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "mixed")
            .load("src/test/resources/aggregates/employees.xml");

        ConnectorException ex = assertThrowsConnectorException(() -> dataset.count());
        String message = ex.getMessage();
        assertTrue(message.startsWith("Unable to get text from URI element 'mixed' found in aggregate element 1 in file"),
            "The error should identify the URI element that text could not be retrieved from along with which aggregate " +
                "element produced the failure; actual message: " + message);
    }

    @Test
    void uriElementNotFound() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "elementDoesntExist")
            .load("src/test/resources/aggregates/employees.xml");

        ConnectorException ex = assertThrowsConnectorException(() -> dataset.count());
        String message = ex.getMessage();
        assertTrue(message.startsWith("No occurrence of URI element 'elementDoesntExist' found in aggregate element 1 in file"),
            "The error should identify which aggregate element did not contain the URI element; actual error: " + message);
    }

    @Test
    void notXmlFile() {
        Dataset<Row> dataset = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "name")
            .load("src/test/resources/500-employees.json");

        ConnectorException ex = assertThrowsConnectorException(() -> dataset.count());
        String message = ex.getMessage();
        assertTrue(message.startsWith("Unable to read XML from file"), "Unexpected error: " + message);
        assertTrue(message.endsWith("500-employees.json; cause: Failed to traverse document"),
            "The error should identify the fail and the root cause; actual error: " + message);
    }

    @Test
    void ignoreUriElementNotFound() {
        long count = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_AGGREGATES_XML_URI_ELEMENT, "id")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load("src/test/resources/aggregates/employees.xml")
            .count();

        assertEquals(2, count, "The one employee without an 'id' element should have caused an error to be " +
            "caught and logged. The 2 employees with 'id' should be returned since abortOnFailure = false.");
    }

    @Test
    void ignoreInvalidXmlFile() {
        long count = newSparkSession().read()
            .format(CONNECTOR_IDENTIFIER)
            .option(Options.READ_NUM_PARTITIONS, 1)
            .option(Options.READ_AGGREGATES_XML_ELEMENT, "Employee")
            .option(Options.READ_FILES_ABORT_ON_FAILURE, false)
            .load("src/test/resources/junit-platform.properties", "src/test/resources/aggregates/employees.xml")
            .count();

        assertEquals(3, count);
    }

    private void verifyRow(Row row, String expectedUriSuffix, String rootPath, String name, int age) {
        String uri = row.getString(0);
        assertTrue(uri.endsWith(expectedUriSuffix), format("URI %s doesn't end with %s", uri, expectedUriSuffix));
        String xml = new String((byte[]) row.get(1));
        XmlNode doc = new XmlNode(xml, Namespace.getNamespace("ex", "org:example"));
        doc.assertElementValue(String.format("%sname", rootPath), name);
        doc.assertElementValue(String.format("%sage", rootPath), Integer.toString(age));
        assertEquals("xml", row.getString(2));
    }
}
