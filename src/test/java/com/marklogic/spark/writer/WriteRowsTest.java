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
import com.marklogic.spark.AbstractIntegrationTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class WriteRowsTest extends AbstractIntegrationTest {

    @Test
    void write200RowsFromCsvFile() {
        SparkSession sparkSession = newSparkSession();

        Dataset<Row> dataset = sparkSession.read()
            .option("header", true)
            .format("csv")
            .csv("src/test/resources/data.csv");


        dataset.write()
            .format("com.marklogic.spark")
            .option("spark.marklogic.client.host", testConfig.getHost())
            .option("spark.marklogic.client.port", testConfig.getRestPort())
            .option("spark.marklogic.client.username", testConfig.getUsername())
            .option("spark.marklogic.client.password", testConfig.getPassword())
            .option("spark.marklogic.client.authType", "digest")
            // Only Append is supported so far, and it must be specified.
            .mode(SaveMode.Append)
            .save();

        final int expectedCollectionSize = 200;
        String uri = getUrisInCollection("my-test-data", expectedCollectionSize).get(0);
        JsonNode doc = readJsonDocument(uri);
        assertTrue(doc.has("docNum"));
        assertTrue(doc.has("docName"));
    }
}
