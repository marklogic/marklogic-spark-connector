/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer;

import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.ConnectorException;
import com.marklogic.spark.Options;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.jdom2.Namespace;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteXmlRowsTest extends AbstractWriteTest {

    @Test
    void rootNameAndNamespace() {
        defaultWrite(newDefaultReader()
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "with-namespace")
            .option(Options.WRITE_XML_ROOT_NAME, "myDoc")
            .option(Options.WRITE_XML_NAMESPACE, "example.org"));

        getUrisInCollection("with-namespace", 15).forEach(uri -> {
            assertTrue(uri.endsWith(".xml"),
                "When an XML root name is specified, the URI suffix should default to .xml; actual URI: " + uri);
            XmlNode doc = readXmlDocument(uri);
            doc.setNamespaces(new Namespace[]{Namespace.getNamespace("ex", "example.org")});
            doc.assertElementExists("/ex:myDoc/ex:Medical.Authors.ForeName");
        });
    }

    @Test
    void noNamespace() {
        defaultWrite(newDefaultReader()
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "no-namespace")
            .option(Options.WRITE_XML_ROOT_NAME, "myDoc"));

        getUrisInCollection("no-namespace", 15).forEach(uri -> {
            assertTrue(uri.endsWith(".xml"));
            readXmlDocument(uri).assertElementExists("/myDoc/Medical.Authors.ForeName");
        });
    }

    /**
     * The URI template feature is a little funky when generating XML documents, because - at least as of 2.3.0 - the
     * source for the template values is a JSON representation of the row. If/when we support XPath expression in a
     * URI template, it would naturally make sense to instead create an XML representation of the row. But for now, our
     * documentation will need to note that regardless of whether the user is generating a JSON or XML document, the
     * URI template source is a JSON representation of the row.
     */
    @Test
    void uriTemplate() {
        defaultWrite(newDefaultReader()
            .option(Options.READ_OPTIC_QUERY, "op.fromView('Medical','Authors', '')")
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_COLLECTIONS, "with-uri-template")
            .option(Options.WRITE_URI_TEMPLATE, "/xml/{ForeName}")
            .option(Options.WRITE_XML_ROOT_NAME, "myDoc")
            .option(Options.WRITE_XML_NAMESPACE, "example.org"));

        assertCollectionSize("with-uri-template", 15);
        XmlNode doc = readXmlDocument("/xml/Appolonia");
        doc.setNamespaces(new Namespace[]{Namespace.getNamespace("ex", "example.org")});
        doc.assertElementValue("/ex:myDoc/ex:LastName", "Edeler");
    }

    @Test
    void xmlRootNameAndJsonRootName() {
        DataFrameWriter writer = newDefaultReader()
            .load()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_JSON_ROOT_NAME, "myJson")
            .option(Options.WRITE_XML_ROOT_NAME, "myDoc")
            .mode(SaveMode.Append);

        ConnectorException ex = assertThrowsConnectorException(writer::save);
        assertEquals("Cannot specify both spark.marklogic.write.jsonRootName and spark.marklogic.write.xmlRootName",
            ex.getMessage());
    }
}
