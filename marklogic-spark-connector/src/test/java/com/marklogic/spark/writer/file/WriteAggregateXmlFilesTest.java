/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteAggregateXmlFilesTest extends AbstractIntegrationTest {

    @Test
    void test(@TempDir Path tempDir) throws IOException {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_URIS, "/utf8-sample.xml\n/pretty-print/doc1.xml")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_AGGREGATES_XML_ELEMENT, "hello")
//            .option(Options.WRITE_FILES_PRETTY_PRINT, true)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length);
        String content = new String(FileCopyUtils.copyToByteArray(tempDir.toFile().listFiles()[0]));

        String expectedContent = "<hello>\n" +
            "<root>\n" +
            "    <hello>world</hello>\n" +
            "</root>\n" +
            "<doc>UTF-8 Text: MaryZhengäöüß测试</doc>\n" +
            "</hello>";
        assertEquals(expectedContent, content, "While each XML document is pretty-printed - which ensures that the " +
            "XML declaration isn't included - the entire document is not pretty-printed. That results in each child " +
            "document not being properly indented. That is expected to be fine for users.");
    }
}
