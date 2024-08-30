/*
 * Copyright © 2024 MarkLogic Corporation. All Rights Reserved.
 */
package com.marklogic.spark.writer.file;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.spark.AbstractIntegrationTest;
import com.marklogic.spark.Options;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteAggregateJsonFilesTest extends AbstractIntegrationTest {

    @Test
    void test(@TempDir Path tempDir) throws IOException {
        newSparkSession()
            .read().format(CONNECTOR_IDENTIFIER)
            .option(Options.CLIENT_URI, makeClientUri())
            .option(Options.READ_DOCUMENTS_COLLECTIONS, "author")
            .load()
            .repartition(1)
            .write().format(CONNECTOR_IDENTIFIER)
            .option(Options.WRITE_AGGREGATES_JSON, true)
            .mode(SaveMode.Append)
            .save(tempDir.toFile().getAbsolutePath());

        assertEquals(1, tempDir.toFile().listFiles().length);
        String content = new String(FileCopyUtils.copyToByteArray(tempDir.toFile().listFiles()[0]));
        System.out.println(content);

        ArrayNode array = (ArrayNode) objectMapper.readTree(content);
        assertEquals(15, array.size(), "Expecting 15 as there are 15 docs in the 'author' collection.");
    }
}
