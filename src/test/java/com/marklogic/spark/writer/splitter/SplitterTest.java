/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.splitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import com.marklogic.spark.writer.JsonUtil;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Text;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class SplitterTest extends AbstractIntegrationTest {

    @Test
    void hacking() {
        ObjectNode node = new ObjectMapper().createObjectNode().put("hello", "world");
        node.putObject("child").put("color", "blue");

        BytesHandle handle = new BytesHandle(node.toString().getBytes());
        JsonNode doc = JsonUtil.getJsonFromHandle(handle);
//        System.out.println(doc.toPrettyString());

        System.out.println(doc.at("#"));
    }

    @Test
    void jsonPath() {
        ObjectNode node = new ObjectMapper().createObjectNode().put("color", "green");
        node.putObject("child").put("color", "blue");
        node.putArray("stuff").add(1).addObject().putArray("moreStuff").add(2);
        Object doc = Configuration.defaultConfiguration().jsonProvider().parse(node.toString());
//        Object output = JsonPath.read(doc, "stuff..moreStuff");
//        System.out.println(output.getClass());
        System.out.println(JsonPath.read(doc, "$.stuff[1].moreStuff[0]").toString());
        System.out.println(JsonPath.read(doc, "$..color").toString());

        if (false) {
            Configuration config = Configuration.builder().jsonProvider(new JacksonJsonNodeJsonProvider()).build();
            DocumentContext context = JsonPath.using(config).parse(node);
//            output = context.read(JsonPath.compile("$.hello.text()"));
//            System.out.println(output);
        }
    }

    /**
     * Hmm - thing we're going to need to let a user register namespaces.
     * spark.marklogic.write.splitter.xml.namespace.foo=org:example .
     *
     * So for XML - the user can choose what they want, in terms of element/text/etc, and we'll have to toString it.
     * 
     * @throws Exception
     */
    @Test
    void xmlPath() throws Exception {
        String xml = "<root>" +
            "<color>green</color>" +
            "<child><color>blue</color></child>" +
            "<nested><content active='true'>Hello world</content></nested>" +
            "</root>";

        XMLOutputter outputter = new XMLOutputter(Format.getRawFormat());
        Document doc = new SAXBuilder().build(new StringReader(xml));
        XPathFactory f = XPathFactory.instance();
        String xpath = "//color[text()]";
        XPathExpression<Object> expr = f.compile(xpath, Filters.fpassthrough());

        expr.evaluate(doc).forEach(thing -> {
            if (thing instanceof Element) {
                System.out.println(outputter.outputString((Element) thing));
            }
            System.out.println(thing.toString());
            System.out.println(thing.getClass());
        });
    }

    @Test
    void test() {
        SplitterDocumentProcessor service = new SplitterDocumentProcessor(
            new JsonTextSelector("/text"),
            DocumentSplitters.recursive(500, 0),
            new SourceDocumentChunkProcessor()
        );

        final String uri = "/marklogic-docs/java-client-intro.json";
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        JacksonHandle contentHandle = getDatabaseClient().newJSONDocumentManager().read(uri, metadata, new JacksonHandle());

        DocumentWriteOperation sourceDocument = new DocumentWriteOperationImpl(uri, metadata, contentHandle);
        DocumentWriteOperation writeOp = service.apply(sourceDocument).next();

        JsonNode doc = JsonUtil.getJsonFromHandle(writeOp.getContent());
        System.out.println(doc.toPrettyString());
        assertSame(sourceDocument, writeOp);
    }

    @Test
    void realTest() {
        readTestDocument()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        assertTrue(doc.has("chunks"));
        assertEquals(4, doc.get("chunks").size(), "Expecting 4 chunks based on prior testing of the langchain4j " +
            "recursive splitter and the amount of text in the test document.");
    }

    @Test
    void customSplitter() {
        readTestDocument()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_SPLITTER_CUSTOM_CLASS, "com.marklogic.spark.writer.splitter.TestCustomSplitter")
            .option(Options.WRITE_SPLITTER_CUSTOM_OPTION_PREFIX + "preamble", "Test: ")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json");
        ArrayNode chunks = doc.withArray("chunks");
        assertEquals(2, chunks.size());
        assertEquals("Test: hello", chunks.get(0).get("text").asText());
        assertEquals("Test: world", chunks.get(1).get("text").asText());
    }

    @Test
    void sidecar() {
        readTestDocument()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_SPLITTER_OUTPUT, "sidecar")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();

        JsonNode doc = readJsonDocument("/split-test.json-chunks.json");
        System.out.println(doc.toPrettyString());
    }

    @Test
    void chunks() {
        readTestDocument()
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.WRITE_SPLITTER_JSON_POINTERS, "/text")
            .option(Options.WRITE_SPLITTER_OUTPUT, "chunks")
            .option(Options.WRITE_PERMISSIONS, DEFAULT_PERMISSIONS)
            .option(Options.WRITE_URI_TEMPLATE, "/split-test.json")
            .mode(SaveMode.Append)
            .save();
    }

    private Dataset<Row> readTestDocument() {
        return newSparkSession().read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/marklogic-docs/java-client-intro.json")
            .load();
    }
}
