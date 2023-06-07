/*
 * Copyright 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.spark.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.junit5.MarkLogicNamespaceProvider;
import com.marklogic.junit5.NamespaceProvider;
import com.marklogic.junit5.XmlNode;
import com.marklogic.spark.Options;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WriteRowsWithTransformTest extends AbstractWriteTest {

    @Override
    protected NamespaceProvider getNamespaceProvider() {
        // Overridden so XmlNode can be used below with the basic JSON namespace.
        return new MarkLogicNamespaceProvider(
            "jb", "http://marklogic.com/xdmp/json/basic"
        );
    }

    /**
     * This also verifies that a user can write XML documents without support from the connector. The user can use a
     * REST transform to convert the JSON representation of a Spark row into whatever XML document they want, along with
     * giving it a more meaningful suffix of ".xml".
     */
    @Test
    void transformToXml() {
        newWriterForSingleRow()
            .option(Options.WRITE_TRANSFORM_NAME, "transformToXml")
            // Surprisingly, the REST API is fine with writing an XML document with a suffix of ".json". But making this
            // a more realistic scenario, as a user would almost certainly want ".xml" instead.
            .option(Options.WRITE_URI_SUFFIX, ".xml")
            .save();

        String uri = getUrisInCollection(COLLECTION, 1).get(0);
        assertTrue(uri.endsWith(".xml"), "Unexpected URI: " + uri);

        XmlNode doc = readXmlDocument(uri);
        String message = "Verifying that one XML element exists, which is sufficient for knowing that the " +
            "JSON sent from the Spark connector was successfully converted into XML";
        doc.assertElementValue(message, "/jb:json/jb:content", "hello world");
    }

    @Test
    void withParams() {
        newWriterForSingleRow()
            .option(Options.WRITE_TRANSFORM_NAME, "withParams")
            .option(Options.WRITE_TRANSFORM_PARAMS, "param1,value1,param2,value2")
            .save();

        String uri = getUrisInCollection(COLLECTION, 1).get(0);
        JsonNode doc = readJsonDocument(uri);
        assertTrue(doc.has("params"), "Expected the withParams transform to toss all the params into the document");
        assertEquals("value1", doc.get("params").get("param1").asText());
        assertEquals("value2", doc.get("params").get("param2").asText());
    }

    @Test
    void withParamsAndCustomDelimiter() {
        newWriterForSingleRow()
            .option(Options.WRITE_TRANSFORM_NAME, "withParams")
            .option(Options.WRITE_TRANSFORM_PARAMS, "param1;value,1;param2;value,2")
            .option(Options.WRITE_TRANSFORM_PARAMS_DELIMITER, ";")
            .save();

        String uri = getUrisInCollection(COLLECTION, 1).get(0);
        JsonNode doc = readJsonDocument(uri);
        assertTrue(doc.has("params"), "Expected the withParams transform to toss all the params into the document");
        assertEquals("value,1", doc.get("params").get("param1").asText());
        assertEquals("value,2", doc.get("params").get("param2").asText());
    }

    @Test
    void invalidTransform() {
        SparkException ex = assertThrows(SparkException.class,
            () -> newWriterForSingleRow()
                .option(Options.WRITE_TRANSFORM_NAME, "this-doesnt-exist")
                .save());

        Throwable cause = getCauseFromWriterException(ex);
        assertTrue(cause instanceof IOException);
        assertTrue(cause.getMessage().contains("Extension this-doesnt-exist or a dependency does not exist"),
            "The connector can't easily validate that a REST transform is valid, but the expectation is that the " +
                "error message from the REST API will make the problem evident to the user; " +
                "unexpected message: " + cause.getMessage());
    }

    @Test
    void invalidTransformParams() {
        SparkException ex = assertThrows(SparkException.class,
            () -> newWriterForSingleRow()
                .option(Options.WRITE_TRANSFORM_NAME, "withParams")
                .option(Options.WRITE_TRANSFORM_PARAMS, "param1,value1,param2")
                .save());

        Throwable cause = getCauseFromWriterException(ex);
        assertTrue(cause instanceof IllegalArgumentException);
        assertEquals(
            "The spark.marklogic.write.transformParams option must contain an equal number of parameter names and values; received: param1,value1,param2",
            cause.getMessage()
        );
    }
}
